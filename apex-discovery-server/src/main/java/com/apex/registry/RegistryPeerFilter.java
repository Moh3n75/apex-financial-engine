package com.apex.registry;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Controlled secure registry mode: CA trust plus exact URI identity and per-service write ownership. */
@Component
@org.springframework.core.annotation.Order(org.springframework.core.Ordered.HIGHEST_PRECEDENCE)
@ConditionalOnProperty(name="apex.discovery.secure-registry.enabled",havingValue="true")
public final class RegistryPeerFilter implements Filter {
  private final String prefix;
  private final Set<String> readers,writers;
  public RegistryPeerFilter(org.springframework.core.env.Environment env) {
    prefix=env.getRequiredProperty("apex.discovery.secure-registry.uri-prefix");
    readers=Set.of(env.getRequiredProperty("apex.discovery.secure-registry.readers").split(","));
    writers=Set.of(env.getRequiredProperty("apex.discovery.secure-registry.writers").split(","));
    if(!prefix.startsWith("spiffe://")||!prefix.endsWith("/cell/")||!env.getProperty("server.ssl.client-auth","").equalsIgnoreCase("need"))throw new IllegalArgumentException("SECURE_REGISTRY_POLICY");
  }
  @Override public void doFilter(ServletRequest request,ServletResponse response,FilterChain chain)throws IOException,ServletException {
    var req=(HttpServletRequest)request;var res=(HttpServletResponse)response;
    try {
      var certificates=(X509Certificate[])req.getAttribute("jakarta.servlet.request.X509Certificate");
      if(!req.isSecure()||certificates==null||certificates.length==0)throw new IllegalArgumentException();
      certificates[0].checkValidity();
      var uris=certificates[0].getSubjectAlternativeNames().stream().filter(s->s.get(0).equals(6)).map(s->s.get(1).toString()).toList();
      if(uris.size()!=1||!uris.getFirst().startsWith(prefix))throw new IllegalArgumentException();
      String peer=uris.getFirst().substring(uris.getFirst().lastIndexOf('/')+1);
      if(!uris.getFirst().equals(prefix+peer))throw new IllegalArgumentException();
      if(!readers.contains(peer)&&!writers.contains(peer))throw new IllegalArgumentException();
      if(!req.getMethod().equals("GET")) {
        String[] parts=req.getRequestURI().split("/");
        if(!writers.contains(peer)||parts.length<4||!parts[1].equals("eureka")||!parts[2].equals("apps")||!parts[3].equalsIgnoreCase(peer))throw new IllegalArgumentException();
      }
    }catch(Exception rejected){res.setStatus(403);return;}
    chain.doFilter(request,response);
  }
}
