package com.apex.compat;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import static org.assertj.core.api.Assertions.*;

class SqlMigrationCompatibilityTest {
    @Test void appliedMigrationsStayImmutableAndInstallTheV1Schema() throws Exception {
        String[] migrations={"V1001__platform_outbox_event.sql","V1002__platform_processed_event.sql"};
        for(String file:migrations) {
            String actual=new ClassPathResource("db/platform/"+file).getContentAsString(StandardCharsets.UTF_8).replace("\r\n","\n");
            String frozen=new String(SdkContractCompatibilityTest.bytes("sql/"+file),StandardCharsets.UTF_8).replace("\r\n","\n");
            assertThat(actual).as("Never rewrite applied migration %s",file).isEqualTo(frozen);
        }
        try(var pg=EmbeddedPostgres.builder().start()) {
            var db=pg.getPostgresDatabase();
            new ResourceDatabasePopulator(Arrays.stream(migrations).map(f->new ClassPathResource("db/platform/"+f)).toArray(ClassPathResource[]::new)).execute(db);
            try(var connection=db.getConnection();var statement=connection.createStatement()) {
                assertThat(columns(connection,"outbox_event")).containsExactly("id:bigserial","event_id:uuid","event_type:varchar",
                        "event_version:int4","correlation_id:uuid","causation_id:uuid","aggregate_id:uuid","aggregate_type:varchar",
                        "source_service:varchar","cell_id:varchar","occurred_at:timestamptz","payload:jsonb","published:bool",
                        "published_at:timestamptz","created_at:timestamptz","retry_count:int4","last_error:text","next_retry_at:timestamptz",
                        "dead_letter:bool","dead_letter_at:timestamptz","claim_token:uuid","claimed_at:timestamptz","claimed_until:timestamptz","publisher_instance_id:varchar");
                assertThat(columns(connection,"processed_event")).containsExactly("consumer_group:varchar","event_id:uuid","event_type:varchar",
                        "event_version:int4","topic:varchar","partition_id:int4","offset_value:int8","processed_at:timestamptz");
                var primary=new ArrayList<String>();try(var result=connection.getMetaData().getPrimaryKeys(null,"public","processed_event")){
                    while(result.next())primary.add(result.getString("COLUMN_NAME"));}
                assertThat(primary).containsExactlyInAnyOrder("consumer_group","event_id");
                assertThatThrownBy(() -> statement.execute("INSERT INTO processed_event VALUES ('group',gen_random_uuid(),'SDK',0,'topic',0,0,now())"))
                        .isInstanceOfSatisfying(SQLException.class,e->assertThat(e.getSQLState()).isEqualTo("23514"));
                statement.execute("INSERT INTO processed_event VALUES ('group','00000000-0000-0000-0000-000000000001','SDK',1,'topic',0,0,now())");
                assertThatThrownBy(() -> statement.execute("INSERT INTO processed_event SELECT * FROM processed_event"))
                        .isInstanceOfSatisfying(SQLException.class,e->assertThat(e.getSQLState()).isEqualTo("23505"));
            }
        }
    }
    private List<String> columns(Connection connection,String table)throws Exception {
        var columns=new ArrayList<String>();try(var result=connection.getMetaData().getColumns(null,"public",table,null)){
            while(result.next())columns.add(result.getString("COLUMN_NAME")+":"+result.getString("TYPE_NAME"));}
        return columns;
    }
}
