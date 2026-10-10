"""Read-only host/media observations; writes no DB and exports no job identity."""
import argparse
import datetime
import json
import pathlib
import re
import shutil
import sqlite3
import os
import tempfile
import subprocess
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def recording_health(database, now=None, grace_seconds=180):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    try: zone = ZoneInfo('Asia/Shanghai')
    except ZoneInfoNotFoundError:
        # Only current/recent schedules are evaluated; China uses UTC+8 without
        # DST. Do not require a Windows timezone package for these local tests.
        zone = datetime.timezone(datetime.timedelta(hours=8), 'Asia/Shanghai')
    local = now.astimezone(zone)
    db = sqlite3.connect(pathlib.Path(database).resolve().as_uri()+'?mode=ro', uri=True, timeout=3)
    try:
        failed = db.execute("SELECT COUNT(*) FROM recording_execution WHERE status='failed' AND started_at>=?",
                            ((now-datetime.timedelta(hours=24)).isoformat().replace('+00:00','Z'),)).fetchone()[0]
        schedules = db.execute('SELECT s.program_id,s.weekday,s.start_time,s.end_time FROM recording_schedule s JOIN program p ON p.id=s.program_id WHERE s.enabled=1 AND p.enabled=1').fetchall()
        missing = 0
        for offset in (-1, 0):
            date = (local+datetime.timedelta(days=offset)).date()
            for program, weekday, start, end in schedules:
                if (date.weekday()+1)%7 != weekday: continue
                begin = datetime.datetime.combine(date, datetime.time.fromisoformat(start), zone)
                finish = datetime.datetime.combine(date, datetime.time.fromisoformat(end), zone)
                if finish <= begin: finish += datetime.timedelta(days=1)
                if now < finish+datetime.timedelta(seconds=grace_seconds): continue
                utc = lambda value: value.astimezone(datetime.timezone.utc).isoformat().replace('+00:00','Z')
                count = db.execute('SELECT COUNT(*) FROM recording_file WHERE program_id=? AND size_bytes>0 AND created_at>=? AND created_at<=?',
                                   (program, utc(begin), utc(finish+datetime.timedelta(seconds=grace_seconds)))).fetchone()[0]
                if count == 0: missing += 1
        return {'failed_last_24h': failed, 'missing_scheduled_windows': missing}
    finally: db.close()


def observations(disks, gate_root=None, recording_db=None):
    result = {'disks': {}, 'gpu': {'available': False}, 'queue': {'available': False}, 'recordings': {'available': False}}
    for label, path in disks.items():
        if not re.fullmatch('[a-zA-Z][a-zA-Z0-9_]{0,31}', label): raise ValueError('Invalid disk label')
        try:
            d = shutil.disk_usage(path)
            result['disks'][label] = {'available': True, 'total_bytes': d.total, 'used_bytes': d.used, 'free_bytes': d.free,
                                      'low_space': d.free < max(4*1024**3, d.total*.05)}
        except OSError: result['disks'][label] = {'available': False}
    try:
        raw = subprocess.check_output(['nvidia-smi', '--query-gpu=index,utilization.gpu,utilization.encoder,utilization.decoder,memory.used,memory.total', '--format=csv,noheader,nounits'], timeout=3, stderr=subprocess.DEVNULL).decode()
        gpus = []
        for line in raw.splitlines():
            values = [int(v.strip()) for v in line.split(',')]
            if len(values) != 6: raise ValueError('Invalid GPU sample')
            gpus.append(dict(zip(('index', 'compute_percent', 'encode_percent', 'decode_percent', 'memory_used_mib', 'memory_total_mib'), values)))
        result['gpu'] = {'available': bool(gpus), 'devices': gpus}
    except (OSError, ValueError, subprocess.SubprocessError): pass
    if gate_root:
        try:
            path = pathlib.Path(gate_root)/'admission.sqlite'
            db = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=3)
            try:
                rows = db.execute('SELECT state,queued_at,started_at FROM admission').fetchall()
            finally: db.close()
            stamp = datetime.datetime.now().timestamp()
            result['queue'] = {'available': True, 'waiting': sum(r[0]=='waiting' for r in rows), 'running': sum(r[0]=='running' for r in rows),
                               'oldest_wait_seconds': max([max(0,stamp-r[1]) for r in rows if r[0]=='waiting' and r[1]] or [0])}
        except (OSError, sqlite3.Error): pass
    if recording_db:
        try: result['recordings'] = {'available': True, **recording_health(recording_db)}
        except (OSError, sqlite3.Error, ValueError): pass
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--disk', action='append', default=[], help='operator-label:absolute-path')
    parser.add_argument('--gate-root')
    parser.add_argument('--recording-db')
    parser.add_argument('--output', type=pathlib.Path, help='Atomic latest JSON in a provisioned private directory')
    args = parser.parse_args()
    raw = json.dumps(observations(dict(v.split(':',1) for v in args.disk), args.gate_root, args.recording_db))
    if args.output:
        if not args.output.parent.is_dir() or args.output.is_symlink(): parser.error('Output directory must be provisioned; symlinks refused')
        with tempfile.NamedTemporaryFile(mode='w', prefix='.monitor-', dir=args.output.parent, delete=False) as f:
            os.fchmod(f.fileno(), 0o640)
            f.write(raw); name = f.name
        try: os.replace(name, args.output)
        finally:
            if os.path.exists(name): os.unlink(name)
    else: print(raw)
