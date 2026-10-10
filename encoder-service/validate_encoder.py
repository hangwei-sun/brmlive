"""Offline same-source A/B. Run manually on GPU host, never via the public API."""
import argparse
import json
import os
import pathlib
import subprocess
import time
from app import encode_args


def probe(path):
    raw = subprocess.check_output(['ffprobe', '-v', 'error', '-show_streams', '-show_format', '-of', 'json', str(path)], timeout=30)
    info = json.loads(raw)
    return [{'type': s['codec_type'], 'duration': s.get('duration'), 'start_time': s.get('start_time'),
             'frames': s.get('nb_frames'), 'frame_rate': s.get('avg_frame_rate')} for s in info['streams']]


def first_frames(source, start, hardware):
    args = ['ffmpeg', '-v', 'error', '-nostdin']
    if hardware: args += ['-hwaccel', 'cuda', '-hwaccel_output_format', 'cuda']
    args += ['-ss', str(start), '-i', str(source), '-frames:v', '3', '-an']
    if hardware: args += ['-vf', 'hwdownload,format=nv12,format=yuv420p']
    else: args += ['-pix_fmt', 'yuv420p']
    args += ['-f', 'framemd5', 'pipe:1']
    raw = subprocess.check_output(args, timeout=60, stderr=subprocess.DEVNULL).decode()
    return [line.split(',')[-1].strip() for line in raw.splitlines() if line and not line.startswith('#')]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=pathlib.Path)
    parser.add_argument('--start', type=float, required=True, help='Non-keyframe speech boundary to validate')
    parser.add_argument('--duration', type=float, default=60)
    parser.add_argument('--output', type=pathlib.Path, required=True, help='A NEW directory; originals are never modified')
    args = parser.parse_args()
    if not args.source.is_file() or args.start < 0 or not 0 < args.duration <= 300: parser.error('Invalid input')
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    results = []
    receipt = {'source_first_3_decode_frames_match': first_frames(args.source, args.start, False)==first_frames(args.source, args.start, True),
               'note': 'Frame hashes compare raw source decoding, NOT lossy encodes. Output SSIM, first-frame visual QA and AV sync still required.'}
    profiles = [('cpu_x264', None, None), ('nvenc_legacy', 'legacy', 'cpu'), ('nvenc_capped', 'capped', 'cpu'),
                ('nvdec_nvenc_legacy', 'legacy', 'nvdec'), ('nvdec_nvenc_capped', 'capped', 'nvdec')]
    for name, profile, decoder in profiles:
        output = args.output/(name+'.mp4')
        if profile is None:
            command = ['ffmpeg','-v','error','-nostdin','-y','-threads','2','-ss',str(args.start),'-i',str(args.source),'-t',str(args.duration),
                       '-map','0:v:0','-map','0:a:0?','-c:v','libx264','-preset','veryfast','-crf','20','-pix_fmt','yuv420p',
                       '-c:a','aac','-b:a','128k','-movflags','+faststart',str(output)]
        else:
            os.environ['ENCODER_PROFILE'], os.environ['ENCODER_DECODE'] = profile, decoder
            command = encode_args(args.source, output, args.start, args.duration)
        started = time.monotonic()
        subprocess.run(command, timeout=1800, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        results.append({'profile': name, 'seconds': round(time.monotonic()-started,3), 'bytes': output.stat().st_size, 'streams': probe(output)})
        receipt['results'] = results
        (args.output/'receipt.json').write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__': main()
