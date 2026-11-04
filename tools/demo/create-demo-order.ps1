. "$PSScriptRoot\common.ps1"
$m = Read-DemoManifest
if ($m.phase -ne 'RUNNING' -or -not (Test-DemoIdentity $m.supervisor)) { throw 'Owned supervisor is not running.' }
$id = [guid]::NewGuid().ToString()
$path = Join-Path $m.runDirectory "commands\$id"
[IO.File]::WriteAllText("$path.tmp", 'CREATE_HAPPY_ORDER')
Move-Item -LiteralPath "$path.tmp" -Destination "$path.request"
$deadline = (Get-Date).AddSeconds(90)
while (-not (Test-Path "$path.response")) {
    if ((Get-Date) -gt $deadline -or -not (Test-DemoIdentity $m.supervisor)) { throw 'Order verification timed out; inspect owned service logs. No synthetic fallback.' }
    Start-Sleep -Milliseconds 500
}
$result = Get-Content "$path.response" -Raw | ConvertFrom-Json
if ($result.error) { throw "Order not verified: $($result.error). See $($m.runDirectory)." }
$result | ConvertTo-Json -Depth 8
Write-Host 'Real Order POST + owner SQL verified. Events: order-cell / owner. Workflows: Order, Inventory, Payment.'
Write-Host 'Search a UTC window covering observedAt (max one hour). For Inbox use the inboxGroup printed above.'
