package com.apex.operations.contracts;

import com.apex.platform.grpc.api.CellQuery;

/** Opt-in extended metadata; v1 callers retain their strict legacy response shape. */
@CellQuery(type="OPS_CONTRACT_DISCOVERY",version=2,response=ContractManifest.class)
public record ContractDiscoveryQueryV2() {}
