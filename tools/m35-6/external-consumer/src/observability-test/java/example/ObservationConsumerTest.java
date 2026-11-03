package example;

import static org.assertj.core.api.Assertions.*;
import com.apex.observability.*;
import io.micrometer.observation.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

class ObservationConsumerTest {
  @Test void optionalDependencyAloneDoesNotActivateDiagnostics(){
    try(var context=new SpringApplicationBuilder(ConsumerApplication.class).web(WebApplicationType.NONE)
        .run("--spring.config.location=optional:classpath:/absent.properties")){
      assertThat(context.getBeansOfType(OperationalObserver.class)).isEmpty();
      assertThat(context.getBeansOfType(ActiveOperations.class)).isEmpty();
    }
  }
  @Test void explicitOptInUsesOneNativeHandlerAndBoundedLocalAdmission(){
    try(var context=new SpringApplicationBuilder(ConsumerApplication.class).web(WebApplicationType.NONE)
        .run("--spring.config.location=optional:classpath:/absent.properties","--apex.observability.enabled=true",
            "--apex.observability.active-enabled=true")){
      assertThat(context.getBeansOfType(OperationalObservationHandler.class)).hasSize(1);
      assertThat(context.getBeansOfType(ObservationBatchExporter.class)).isEmpty();
      var observation=Observation.start("apex.query",context.getBean(ObservationRegistry.class));
      assertThat(context.getBean(ActiveOperations.class).size()).isEqualTo(1);observation.stop();
      assertThat(context.getBean(ActiveOperations.class).size()).isZero();
      assertThat(context.getBean(BoundedOperationalObserver.class).drain(64)).hasSize(1);
    }
  }
}
