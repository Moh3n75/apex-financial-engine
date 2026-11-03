package com.apex.operations.owner;

import com.apex.platform.grpc.api.GatewayException;
import java.sql.*;
import java.util.*;

/** Owner-authored names only. Checks valid ready nonpartial btree definition on every bounded read. */
public final class RecentIndex {
    private RecentIndex(){}
    public static void require(Connection c,String table,List<String> columns)throws SQLException {
        try(var s=c.prepareStatement("SELECT i.indisvalid AND i.indisready AND i.indpred IS NULL AND i.indexprs IS NULL AND a.amname='btree' AND i.indnkeyatts=? AND i.indnatts=?, ARRAY(SELECT pg_get_indexdef(i.indexrelid,n,true) || CASE WHEN pg_index_column_has_property(i.indexrelid,n,'desc') THEN ' DESC' ELSE '' END FROM generate_series(1,i.indnkeyatts) n) FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace ns ON ns.oid=t.relnamespace JOIN pg_am a ON a.oid=x.relam WHERE ns.nspname='public' AND t.relname=? AND x.relname=?")) {
            s.setQueryTimeout(1);s.setInt(1,columns.size());s.setInt(2,columns.size());s.setString(3,table);s.setString(4,"ops03d_"+table+"_recent");
            try(var r=s.executeQuery()) {
                if(!r.next()||!r.getBoolean(1)||!Arrays.asList((String[])r.getArray(2).getArray()).equals(columns))
                    throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION,false);
            }
        }
    }
}
