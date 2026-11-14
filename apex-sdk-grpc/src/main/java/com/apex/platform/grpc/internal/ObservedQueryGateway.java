package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.*;
import java.util.concurrent.CompletionStage;

final class ObservedQueryGateway implements QueryGateway {
    private final QueryGateway delegate;
    private final RpcObservation observations;
    ObservedQueryGateway(QueryGateway delegate,RpcObservation observations){this.delegate=delegate;this.observations=observations;}
    public <R> R query(String destination,Object query,Class<R> type,QueryOptions options){
        return RpcErrors.await(queryAsync(destination,query,type,options),options.deadline(),false);
    }
    public <R> CompletionStage<R> queryAsync(String destination,Object query,Class<R> type,QueryOptions options){
        return observations.watch("query",()->delegate.queryAsync(destination,query,type,options));
    }
    public <R> R queryLocal(Object query,Class<R> type,QueryOptions options){
        var attempt=observations.start("query");
        try{var result=delegate.queryLocal(query,type,options);attempt.finish("SUCCEEDED");return result;}
        catch(RuntimeException failure){attempt.finish(RpcObservation.outcome(failure));throw failure;}
    }
}
