param(
  [string]$MavenCommand = (Join-Path $env:USERPROFILE '.m2/wrapper/dists/apache-maven-3.9.11-bin/6mqf5t809d9geo83kj4ttckcbc/apache-maven-3.9.11/bin/mvn.cmd'),
  [switch]$Offline
)
$ErrorActionPreference = 'Stop'
$taskRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$taskOffline = @()
if ($Offline) { $taskOffline = @('-o') }
function Invoke-ContractMaven([string[]]$MavenArgs) {
  $taskAllArgs = @($taskOffline) + @($MavenArgs)
  & $MavenCommand @taskAllArgs
  if ($LASTEXITCODE -ne 0) { throw 'Contract verification failed; see Maven output.' }
}
Push-Location $taskRoot
try {
  Invoke-ContractMaven -MavenArgs @('-q','-pl','apex-platform-contract-discovery,apex-starter-grpc-server,apex-starter-cell-messaging,apex-operations-owner-jdbc','-am','install','-DskipTests')
  foreach ($taskCell in @('order-cell','inventory-cell','payment-cell','customer-cell','reporting-cell')) {
    $taskPom = Join-Path $taskRoot "$taskCell/pom.xml"
    if ($taskCell -eq 'order-cell') {
      # Existing unrelated OrderOperationsPropertiesTest references a staged-deleted class.
      # Isolate compilation to this test without modifying the real POM or unrelated test.
      $taskTemporaryPom = Join-Path $taskRoot 'order-cell/.control-center-test-pom.xml'
      if (Test-Path -LiteralPath $taskTemporaryPom) { throw 'Temporary verification POM already exists.' }
      $taskXml = (Get-Content -LiteralPath $taskPom -Raw).Replace('<plugins>', '<plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><configuration><testIncludes><testInclude>**/RepositoryContractDiscoveryTest.java</testInclude></testIncludes></configuration></plugin>')
      [System.IO.File]::WriteAllText($taskTemporaryPom, $taskXml)
      try { Invoke-ContractMaven -MavenArgs @('-q','-f',$taskTemporaryPom,'test','-Dtest=RepositoryContractDiscoveryTest') }
      finally { Remove-Item -LiteralPath $taskTemporaryPom }
    } else {
      Invoke-ContractMaven -MavenArgs @('-q','-f',$taskPom,'test','-Dtest=RepositoryContractDiscoveryTest')
    }
    Copy-Item -LiteralPath (Join-Path $taskRoot "$taskCell/target/contract-manifest.json") -Destination (Join-Path $taskRoot "apex-operations-console/src/test/resources/contracts/$taskCell.json")
  }
  Invoke-ContractMaven -MavenArgs @('-q','-pl','apex-platform-contract-discovery,apex-operations-console','-am','test','-Dtest=ContractDiscoveryProviderTest,ContractTopologyTest,RepositoryTopologyIntegrationTest,TopologyApiTest,OpsM36aHttpSecurityIT','-Dsurefire.failIfNoSpecifiedTests=false','-Dapex.verifyRepositoryManifests=true')
  Copy-Item -LiteralPath (Join-Path $taskRoot 'apex-operations-console/target/repository-topology-example.json') -Destination (Join-Path $taskRoot 'docs/examples/control-center-topology-v2.json')
} finally { Pop-Location }
