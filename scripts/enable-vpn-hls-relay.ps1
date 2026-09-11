param(
  [string]$ListenPort = '18080',
  [string]$TargetAddress = '192.0.2.60',
  [string]$TargetPort = '80'
)

# Docker Desktop does not inherit the Windows VPN route. Forward the HLS HTTP
# port through Windows when the source is reachable only from the host network.
netsh interface portproxy delete v4tov4 listenaddress=0.0.0.0 listenport=$ListenPort | Out-Null
netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=$ListenPort connectaddress=$TargetAddress connectport=$TargetPort
netsh interface portproxy show v4tov4
