package com.apex.operations.owner;

import com.apex.operations.contracts.OwnerRecentSearch.Query;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Fixed boot-local DESC tuple pagination. Never a revision, commit coordinate or grant. */
public final class RecentCursor {
    public record Position(String key, Instant time, int page, long expiresAt) {}
    private final byte[] secret=new byte[32];
    private final Clock clock;
    public RecentCursor(Clock clock){this.clock=clock;new SecureRandom().nextBytes(secret);}
    public Position read(Query q) {
        if(q.cursor().isEmpty())return new Position("",null,1,clock.millis()+30000);
        try {
            String[] parts=q.cursor().split("\\.",-1);if(parts.length!=2)throw new IllegalArgumentException();
            byte[] data=Base64.getUrlDecoder().decode(parts[0]),mac=Base64.getUrlDecoder().decode(parts[1]);
            if(!encode(data).equals(parts[0])||!encode(mac).equals(parts[1])||!MessageDigest.isEqual(sign(data),mac))throw new IllegalArgumentException();
            try(var input=new DataInputStream(new ByteArrayInputStream(data))) {
                if(!MessageDigest.isEqual(input.readNBytes(32),binding(q)))throw new IllegalArgumentException();
                long expires=input.readLong();int page=input.readInt();
                Instant time=Instant.ofEpochSecond(input.readLong(),input.readInt());String key=input.readUTF();
                if(input.available()!=0||expires<clock.millis()||expires>clock.millis()+30000||page<2||page>8
                        ||!key.matches("[a-f0-9-]{1,36}")||time.isBefore(q.fromInclusive())||!time.isBefore(q.toExclusive()))throw new IllegalArgumentException();
                return new Position(key,time,page,expires);
            }
        }catch(Exception invalid){throw new IllegalArgumentException("RESTART_WALK_REQUIRED");}
    }
    public String next(Query q,Position p,String key,Instant time) {
        if(p.page()>=8)return "";
        try {
            var bytes=new ByteArrayOutputStream();try(var out=new DataOutputStream(bytes)) {
                out.write(binding(q));out.writeLong(p.expiresAt());out.writeInt(p.page()+1);
                out.writeLong(time.getEpochSecond());out.writeInt(time.getNano());out.writeUTF(key);
            }
            byte[] data=bytes.toByteArray();return encode(data)+"."+encode(sign(data));
        }catch(IOException impossible){throw new IllegalStateException("RECENT_CURSOR");}
    }
    private byte[] binding(Query q) {
        try{return MessageDigest.getInstance("SHA-256").digest(q.binding().getBytes(StandardCharsets.US_ASCII));}
        catch(GeneralSecurityException impossible){throw new IllegalStateException("RECENT_CRYPTO");}
    }
    private byte[] sign(byte[] data) {
        try{var mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(secret,"HmacSHA256"));return mac.doFinal(data);}
        catch(GeneralSecurityException impossible){throw new IllegalStateException("RECENT_CRYPTO");}
    }
    private static String encode(byte[] b){return Base64.getUrlEncoder().withoutPadding().encodeToString(b);}
}
