param(
    [switch]$WithBurst,
    [int]$BurstRate = 5000
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

function Start-Terminal {
    param([string]$Title, [string]$WorkDir, [string]$Cmd)
    Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
`$host.UI.RawUI.WindowTitle = '$Title'
Set-Location '$WorkDir'
$Cmd
"@
}

Write-Host ""
Write-Host "  FAB WORLD 기동" -ForegroundColor Cyan
Write-Host ""

Start-Terminal "[1] HTML Server"     $root              "py -3.12 -m http.server 5500"
Start-Sleep -Milliseconds 300
Start-Terminal "[2] Producer"        "$root\producer"   "py -3.12 -m uvicorn main:app --reload --port 8000"
Start-Sleep -Milliseconds 300
Start-Terminal "[3] Processor"       "$root\processor"  "py -3.12 main.py"
Start-Sleep -Milliseconds 300
Start-Terminal "[4] CH Consumer"     "$root\consumer"   "py -3.12 ch_consumer.py"

if ($WithBurst) {
    Start-Sleep -Milliseconds 300
    Start-Terminal "[5] Burst Generator" "$root\burst"  "py -3.12 burst_generator.py --rate $BurstRate"
}

Write-Host "  터미널 4개 실행됨" -ForegroundColor Green
Write-Host ""
Write-Host "  Simulator : http://localhost:5500/fab-world.html"
Write-Host "  Grafana   : http://localhost:3000"
Write-Host "  Kafka-UI  : http://localhost:8080"
Write-Host ""
Write-Host "  버스트 테스트: .\run.ps1 -WithBurst" -ForegroundColor DarkGray
Write-Host "  속도 지정:     .\run.ps1 -WithBurst -BurstRate 1000" -ForegroundColor DarkGray
Write-Host ""