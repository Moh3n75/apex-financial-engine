package external.neutral;

import com.apex.platform.messaging.jdbc.ConfigurableOutboxPublisher;
import com.apex.platform.messaging.outbox.OutboxStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods=false)
@EnableAutoConfiguration
public class NeutralConsumer {
    public static void main(String[] args) throws Exception {
        // Compile/link orchestration and storage contracts from the installed provider-neutral path.
        if (ConfigurableOutboxPublisher.class.getConstructors().length != 3 || !OutboxStore.class.isInterface())
            throw new IllegalStateException("Messaging contract mismatch");
        for (String driver : new String[]{"org.postgresql.Driver", "oracle.jdbc.OracleDriver"}) {
            try {Class.forName(driver);throw new IllegalStateException("Unselected database driver: " + driver);}
            catch (ClassNotFoundException expected) {}
        }
        var app=new SpringApplication(NeutralConsumer.class);app.setWebApplicationType(WebApplicationType.NONE);
        try(var context=app.run(args)) {
            if (!context.getBeansOfType(javax.sql.DataSource.class).isEmpty()) throw new IllegalStateException("Unexpected DataSource");
            System.out.println("NEUTRAL_MESSAGING_CONSUMER_OK");
        }
    }
}
