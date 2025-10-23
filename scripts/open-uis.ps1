# scripts/open-uis.ps1
$ErrorActionPreference = 'SilentlyContinue'

# Try Hadoop 3.x (9870) rồi 2.x (50070)
$nnPorts = 9870, 50070
$openedNN = $false
foreach ($p in $nnPorts) {
  try {
    (Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$p" -TimeoutSec 5) | Out-Null
    # Explorer path (nếu có)
    $url = if ($p -eq 50070) { "http://localhost:$p/explorer.html#/" } else { "http://localhost:$p" }
    Start-Process $url
    $openedNN = $true
    break
  } catch {}
}
if (-not $openedNN) { Write-Host "NameNode UI chưa sẵn sàng trên 9870/50070." }

# YARN RM (8088) – báo nếu chưa publish cổng
try {
  (Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8088" -TimeoutSec 3) | Out-Null
  Start-Process "http://localhost:8088"
} catch {
  Write-Host "YARN UI không mở được trên http://localhost:8088 (có thể container resourcemanager chưa publish cổng)."
}

# Presto (8080)
Start-Process "http://localhost:8080"
