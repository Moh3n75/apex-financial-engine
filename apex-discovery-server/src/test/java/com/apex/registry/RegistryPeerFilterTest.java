package com.apex.registry;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.*;
import java.security.cert.X509Certificate;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.mock.web.*;

class RegistryPeerFilterTest {
  @Test void tlsIdentityAndOwnedWritesAreEnforcedBeforeJersey()throws Exception {
    var env=new MockEnvironment().withProperty("apex.discovery.secure-registry.uri-prefix","spiffe://apex.test/tenant/alpha/cell/")
        .withProperty("apex.discovery.secure-registry.readers","console").withProperty("apex.discovery.secure-registry.writers","order-cell").withProperty("server.ssl.client-auth","need");
    var filter=new RegistryPeerFilter(env);
    for(String peer:List.of("console","order-cell","observer"))for(String method:List.of("GET","POST")) {
      var cert=mock(X509Certificate.class);when(cert.getSubjectAlternativeNames()).thenReturn(List.of(List.of(6,"spiffe://apex.test/tenant/alpha/cell/"+peer)));
      var request=new MockHttpServletRequest(method,"/eureka/apps/ORDER-CELL");request.setSecure(true);request.setAttribute("jakarta.servlet.request.X509Certificate",new X509Certificate[]{cert});
      var response=new MockHttpServletResponse();filter.doFilter(request,response,(a,b)->((jakarta.servlet.http.HttpServletResponse)b).setStatus(204));
      assertThat(response.getStatus()).isEqualTo(peer.equals("observer")||method.equals("POST")&&!peer.equals("order-cell")?403:204);
    }
    assertThat(RegistryPeerFilter.class.getAnnotation(org.springframework.core.annotation.Order.class).value()).isEqualTo(Integer.MIN_VALUE);
  }
}
