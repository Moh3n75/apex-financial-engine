param(
 [ValidateRange(1024,65535)][int]$Port=9190,
 [ValidatePattern('^[A-Za-z0-9._-]{1,64}$')][string]$Environment='test',
 [ValidateSet('order-cell','inventory-cell','payment-cell')][string]$Owner='order-cell',
 [ValidateSet('events','workflows','inbox','requests')][string]$Kind='events',
 [ValidatePattern('^[A-Za-z0-9._-]{1,200}$')][string]$Scope='owner',
 [ValidateRange(1,3)][int]$Cycles=2,
 [ValidateRange(1,8)][int]$MaxPages=3,
 [ValidateRange(1,50)][int]$LookbackMinutes=5
)
$ErrorActionPreference='Stop'
$base="http://127.0.0.1:$Port/ops/v1/recent/$Kind"
$from=[DateTimeOffset]::UtcNow.AddMinutes(-$LookbackMinutes).UtcDateTime.ToString('o')
$all=@{}
# Respect the existing two-second client backoff left by a preceding operator read.
# This reduces read pressure; no owner/client deadline or assertion changes.
Start-Sleep -Milliseconds 2100
for($cycle=1;$cycle -le $Cycles;$cycle++) {
 $to=[DateTimeOffset]::UtcNow.UtcDateTime.ToString('o');$cursor=''
 for($page=1;$page -le $MaxPages;$page++) {
  $uri="$base`?environment=$Environment&owner=$Owner&scope=$Scope&from=$from&to=$to&limit=16&cursor=$cursor"
  $wire=Invoke-WebRequest -Uri $uri -Method Get -TimeoutSec 3 -UseBasicParsing
  if([Text.Encoding]::UTF8.GetByteCount($wire.Content) -gt 65536){throw 'HTTP_RESPONSE_BOUND'}
  $view=$wire.Content|ConvertFrom-Json
  if($view.page){
   foreach($item in $view.page.items){$all[$item.evidenceKey]=$item}
   $cursor=$view.page.nextCursor
   [pscustomobject]@{cycle=$cycle;page=$page;mode='OWNER_DIRECT';freshness=$view.freshness;availability=$view.availability;asOf=$view.page.asOf;coverage=($view.page.coverage -join ',');candidates=$view.page.candidatesExamined;items=$view.page.items;relation=$view.page.relation}|ConvertTo-Json -Depth 6 -Compress
  }else{
   [pscustomobject]@{cycle=$cycle;page=$page;mode='OWNER_DIRECT';freshness=$view.freshness;availability=$view.availability;coverage=$view.coverage;items=@()}|ConvertTo-Json -Depth 6 -Compress
   break
  }
  if(!$cursor){break}
  # Separate pages remain within the existing one-admitted-read/owner/second policy.
  Start-Sleep -Milliseconds 1100
 }
 if($cursor){Write-Output 'PARTIAL_CLIENT_PAGE_BUDGET_REACHED'}
 if($cycle -lt $Cycles){Start-Sleep -Milliseconds 2100}
}
Write-Output ('Distinct observed metadata identities: '+$all.Count+'; partial current state only, no durable projection or history.')
