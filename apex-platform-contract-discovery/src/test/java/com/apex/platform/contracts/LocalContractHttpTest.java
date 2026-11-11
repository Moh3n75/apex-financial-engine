package com.apex.platform.contracts;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import com.apex.platform.messaging.jdbc.PublisherRouting;

class LocalContractHttpTest {
  @Test void broadCellComponentScanningCannotBypassTheGuardedFactoryOrDuplicateTheMapping() {
    var scanner=new org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider(true);
    assertThat(scanner.findCandidateComponents("com.apex.platform.contracts")).noneSatisfy(bean->
        assertThat(bean.getBeanClassName()).contains("LocalContractRead"));
  }
  @Test void readUsesExistingProviderAndRejectsRemoteRequestsAndUnsafeBinds() throws Exception {
    var env=new MockEnvironment().withProperty("spring.application.name","sample-cell").withProperty("apex.cell.id","sample-cell")
        .withProperty("apex.contract-discovery.environment","local").withProperty("server.address","127.0.0.1");
    var provider=new ContractDiscoveryProvider(env,new PublisherRouting(false,List.of()),null,null,List.of());
    var config=new LocalContractHttpAutoConfiguration();
    var mvc=MockMvcBuilders.standaloneSetup(config.localContractRead(provider,env)).build();
    mvc.perform(get("/ops/v1/contracts")).andExpect(status().isOk()).andExpect(jsonPath("$.service").value("sample-cell"))
        .andExpect(jsonPath("$.sourceMode").value("EFFECTIVE_DECLARATION"));
    mvc.perform(get("/ops/v1/contracts").with(r->{r.setRemoteAddr("192.0.2.1");return r;})).andExpect(status().isForbidden());
    mvc.perform(post("/ops/v1/contracts")).andExpect(status().isMethodNotAllowed());
    assertThatThrownBy(()->config.localContractRead(provider,env.withProperty("server.address","0.0.0.0"))).hasMessageContaining("LOOPBACK");
    env.withProperty("server.address","127.0.0.1").withProperty("apex.contract-discovery.environment","prod");
    assertThatThrownBy(()->config.localContractRead(provider,env)).hasMessageContaining("LOCAL_ENVIRONMENT");
  }
}
