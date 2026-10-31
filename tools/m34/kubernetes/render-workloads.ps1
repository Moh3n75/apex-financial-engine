param([string]$OutputFile='out/m34-kubernetes.json')
# Render only. Does not contact a cluster, provision Kafka/DB, or create credentials.
$ErrorActionPreference='Stop'
$items=[Collections.Generic.List[object]]::new()
foreach($cell in @('order','inventory','payment')) {
    $id="$cell-cell"; $authority="$id.apex-m34.svc.cluster.local"
    $items.Add(@{apiVersion='v1';kind='Service';metadata=@{name=$id;namespace='apex-m34';labels=@{'apex.cell-id'=$id};annotations=@{'apex.grpc.authority'=$authority}};
        spec=@{selector=@{app=$id};ports=@(@{name='http';port=8080;targetPort='http'},@{name='grpc';port=9090;targetPort='grpc'})}})
    $profiles=if($cell -eq 'payment'){'three-cell-saga,simulation,grpc'}else{'saga,three-cell-saga,grpc'}
    # Separate pre-provisioned secrets, one per owner. No shared business database.
    $items.Add(@{apiVersion='apps/v1';kind='Deployment';metadata=@{name=$id;namespace='apex-m34'};spec=@{
        replicas=$(if($cell -eq 'order'){3}else{2});selector=@{matchLabels=@{app=$id}};template=@{
            metadata=@{labels=@{app=$id}};spec=@{automountServiceAccountToken=$false;terminationGracePeriodSeconds=30;
                securityContext=@{runAsNonRoot=$true;runAsUser=10001;fsGroup=10001};containers=@(@{
                    name=$id;image="apex/$id`:m34-kubernetes";imagePullPolicy='IfNotPresent';
                    ports=@(@{name='http';containerPort=8080},@{name='grpc';containerPort=9090});
                    envFrom=@(@{secretRef=@{name="$id-runtime"}});
                    env=@(@{name='SPRING_PROFILES_ACTIVE';value=$profiles},@{name='SPRING_MAIN_WEB_APPLICATION_TYPE';value='servlet'},
                        @{name='SPRING_CONFIG_ADDITIONAL_LOCATION';value='file:/etc/apex/cell.properties'},
                        @{name='SERVER_PORT';value='8080'},@{name='SERVER_ADDRESS';value='0.0.0.0'},
                        @{name='CELL_GRPC_ADDRESS';value='0.0.0.0'},@{name='CELL_GRPC_PORT';value='9090'});
                    volumeMounts=@(@{name='configuration';mountPath='/etc/apex';readOnly=$true},@{name='tls';mountPath='/etc/apex-tls';readOnly=$true},@{name='tmp';mountPath='/tmp'});
                    readinessProbe=@{httpGet=@{path='/actuator/health/readiness';port='http'};periodSeconds=5;failureThreshold=2};
                    livenessProbe=@{httpGet=@{path='/actuator/health/liveness';port='http'};periodSeconds=10;failureThreshold=3};
                    startupProbe=@{httpGet=@{path='/actuator/health/liveness';port='http'};periodSeconds=5;failureThreshold=30};
                    resources=@{requests=@{cpu='250m';memory='384Mi'};limits=@{cpu='2';memory='768Mi'}};
                    securityContext=@{allowPrivilegeEscalation=$false;readOnlyRootFilesystem=$true;capabilities=@{drop=@('ALL')}}});
                volumes=@(@{name='configuration';configMap=@{name="$id-configuration"}},@{name='tls';secret=@{secretName="$id-mtls"}},@{name='tmp';emptyDir=@{}})
            }}}})
}
$parent=Split-Path $OutputFile -Parent;if($parent){New-Item -ItemType Directory $parent -Force | Out-Null}
[IO.File]::WriteAllText([IO.Path]::GetFullPath($OutputFile),(@{apiVersion='v1';kind='List';items=$items.ToArray()} | ConvertTo-Json -Depth 30),[Text.UTF8Encoding]::new($false))
Write-Output "Rendered $OutputFile; no cluster operations performed."
