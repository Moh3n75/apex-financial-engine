package com.apex.order.modules.order.operations;
import com.zaxxer.hikari.*;import javax.sql.DataSource;
import org.springframework.context.annotation.*;import org.springframework.core.env.Environment;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
@Configuration(proxyBeanMethods=false)
@ConditionalOnProperty(name="apex.ops03.owner-enabled",havingValue="true")
public class OrderEvidenceDataSource {
 public record Pool(HikariDataSource dataSource) implements AutoCloseable { public void close(){dataSource.close();} }
 @Bean(destroyMethod="close") Pool evidencePool(Environment e){
 var c=new HikariConfig();c.setJdbcUrl(e.getRequiredProperty("apex.ops03.jdbc-url"));
 c.setUsername(e.getRequiredProperty("apex.ops03.jdbc-user"));c.setPassword(e.getRequiredProperty("apex.ops03.jdbc-password"));
 c.setMaximumPoolSize(1);c.setMinimumIdle(0);c.setConnectionTimeout(300);c.setValidationTimeout(250);c.setInitializationFailTimeout(-1);c.setReadOnly(true);c.setPoolName("ops03-owner-read");return new Pool(new HikariDataSource(c));
 }
}
