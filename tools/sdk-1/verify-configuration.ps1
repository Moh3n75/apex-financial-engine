param([switch]$Capture,[string]$Root=(Join-Path $PSScriptRoot '../..'))
$ErrorActionPreference='Stop'
$root=(Resolve-Path $Root).Path
$baseline=Join-Path $PSScriptRoot 'baseline/configuration-v1.json'
$catalog=@()
foreach($module in Get-ChildItem $root -Directory -Filter 'apex-sdk-*' | Sort-Object Name) {
    $metadata=Join-Path $module.FullName 'target/classes/META-INF/spring-configuration-metadata.json'
    if(Test-Path $metadata) {
        $document=Get-Content -LiteralPath $metadata -Raw | ConvertFrom-Json
        foreach($property in @($document.properties) | Sort-Object name) {
            $default=if($property.PSObject.Properties.Name -contains 'defaultValue') {ConvertTo-Json -InputObject $property.defaultValue -Depth 8 -Compress}else{'<not-declared>'}
            $catalog+=[ordered]@{module=$module.Name;name=$property.name;type=$property.type;default=$default;description=$property.description}
        }
    }
    # Additional metadata must be merged into the generated file, not just copied beside it.
    $additional=Join-Path $module.FullName 'src/main/resources/META-INF/additional-spring-configuration-metadata.json'
    if(Test-Path $additional) {
        foreach($property in (Get-Content $additional -Raw | ConvertFrom-Json).properties) {
            if(-not @($catalog | Where-Object {$_.module -eq $module.Name -and $_.name -eq $property.name}).Count){throw "Missing generated metadata: $($property.name)"}
        }
    }
}
if($catalog.Count -lt 70){throw 'Incomplete generated configuration metadata; clean compile SDK first.'}
$json=ConvertTo-Json -InputObject @($catalog) -Depth 8
New-Item -ItemType Directory -Force (Join-Path $root 'out/sdk-1') | Out-Null
$json | Set-Content (Join-Path $root 'out/sdk-1/configuration-current.json')
if($Capture){
    if(Test-Path $baseline){throw 'Reviewed baseline already exists; do not silently overwrite it.'}
    $json | Set-Content $baseline
} elseif(($json -replace "`r`n","`n").Trim() -ne ((Get-Content $baseline -Raw) -replace "`r`n","`n").Trim()) {
    throw 'Configuration names/types/defaults/descriptions changed; review versioned contract and current metadata.'
}
# These namespace-like strings are metadata fields, not configuration keys.
$metadataOnly=@('apex.environment','apex.epoch','apex.instance-id','apex.cell-id','apex.grpc.port','apex.grpc.authority','apex.grpc.enabled',
    'apex.observability.admitted','apex.observability.dropped','apex.observability.export.failures',
    'apex.observability.queue.accounted.bytes','apex.observability.queue.records','apex.ops.active','apex.ops.start','apex.outcome')
$names=@($catalog | ForEach-Object {$_.name})
$missing=@()
function VerifyLiteral($Module,$Name,$Literal) {
    $entries=@($catalog | Where-Object {$_.name -eq $Name})
    if(-not $entries.Count){return}
    $owned=@($entries | Where-Object {$_.module -eq $Module})
    $entry=if($owned.Count){$owned[0]}else{$entries[0]}
    if($entry.default -eq '<not-declared>'){return}
    $actual=$Literal -replace '[lL]$',''
    if($actual -ne $entry.default){throw "Runtime literal default differs from metadata: $Module $Name actual=$actual metadata=$($entry.default)"}
}
foreach($file in Get-ChildItem (Join-Path $root 'apex-sdk-*/src/main/java') -Recurse -Filter '*.java') {
    $source=Get-Content -LiteralPath $file.FullName -Raw
    $module=$file.FullName.Substring($root.Length+1).Split([IO.Path]::DirectorySeparatorChar)[0]
    foreach($match in [regex]::Matches($source,'getProperty\s*\(\s*"(apex\.[a-z0-9.-]+)"\s*,\s*(?:[\w.]+\.class\s*,\s*)?(true|false|-?\d+[lL]?|"(?:[^"\\]|\\.)*")\s*\)')) {
        VerifyLiteral $module $match.Groups[1].Value $match.Groups[2].Value
    }
    foreach($match in [regex]::Matches($source,'\$\{(apex\.[a-z0-9.-]+):([^}]+)\}')) {
        $entry=@($catalog | Where-Object {$_.name -eq $match.Groups[1].Value})
        if($entry.Count){
            $literal=$match.Groups[2].Value
            if($entry[0].type -notin @('java.lang.Boolean','java.lang.Long','java.lang.Integer','java.lang.Short')){$literal=ConvertTo-Json -InputObject $literal -Compress}
            VerifyLiteral $module $match.Groups[1].Value $literal
        }
    }
    foreach($match in [regex]::Matches($source,'"(apex\.[a-z][a-z0-9.-]+)"')) {
        $name=$match.Groups[1].Value
        if($name.EndsWith('.') -or $name -in $metadataOnly){continue}
        if(-not @($names | Where-Object {$_ -eq $name -or $_.StartsWith($name+'.')}).Count){$missing+=$name}
    }
}
# ConditionalOnProperty prefix/name pairs and @Value expressions are additionally catalogued manually.
if($missing.Count){throw "Uncatalogued apex namespace/key literals: $(($missing | Sort-Object -Unique)-join ', ')"}
Write-Host "Configuration baseline PASS: $($catalog.Count) module/property entries"
