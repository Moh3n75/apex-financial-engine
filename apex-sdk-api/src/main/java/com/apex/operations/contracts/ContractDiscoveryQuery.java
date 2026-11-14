package com.apex.operations.contracts;

import com.apex.platform.grpc.api.CellQuery;

@CellQuery(type = "OPS_CONTRACT_DISCOVERY", version = 1, response = ContractManifest.class)
public record ContractDiscoveryQuery() {}
