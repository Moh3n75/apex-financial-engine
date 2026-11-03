package com.apex.operations.owner;

import com.apex.operations.contracts.OwnerSearch.Query;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Per-owner boot secret. A cursor is pagination state, never an authorization grant. */
public final class SearchCursor {
    public record Position(String key, int page, long expiresAt) {}
    private final byte[] secret = new byte[32];
    private final Clock clock;
    public SearchCursor(Clock clock) { this.clock=clock; new SecureRandom().nextBytes(secret); }
    public Position read(Query query) {
        if (query.cursor().isEmpty()) return new Position("",1,clock.millis()+30000);
        try {
            var parts=query.cursor().split("\\.",-1);
            if(parts.length!=2) throw new IllegalArgumentException();
            byte[] data=Base64.getUrlDecoder().decode(parts[0]);
            if(!MessageDigest.isEqual(sign(data),Base64.getUrlDecoder().decode(parts[1]))) throw new IllegalArgumentException();
            String[] values=new String(data,StandardCharsets.US_ASCII).split(":",-1);
            if(values.length!=4 || !values[0].equals(binding(query))) throw new IllegalArgumentException();
            long expiry=Long.parseLong(values[1]); int page=Integer.parseInt(values[2]);
            if(expiry<clock.millis() || expiry>clock.millis()+30000 || page<2 || page>8
                    || !values[3].matches("[a-f0-9-]{1,36}")) throw new IllegalArgumentException();
            return new Position(values[3],page,expiry);
        } catch(Exception invalid) { throw new IllegalArgumentException("SEARCH_CURSOR"); }
    }
    public String next(Query query, Position position, String key) {
        if(position.page()>=8) return "";
        byte[] data=(binding(query)+":"+position.expiresAt()+":"+(position.page()+1)+":"+key).getBytes(StandardCharsets.US_ASCII);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data)+"."+Base64.getUrlEncoder().withoutPadding().encodeToString(sign(data));
    }
    private String binding(Query q) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(q.binding().getBytes(StandardCharsets.US_ASCII))); }
        catch(GeneralSecurityException impossible) { throw new IllegalStateException("SEARCH_CRYPTO"); }
    }
    private byte[] sign(byte[] data) {
        try { var mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(secret,"HmacSHA256"));return mac.doFinal(data); }
        catch(GeneralSecurityException impossible) { throw new IllegalStateException("SEARCH_CRYPTO"); }
    }
}
