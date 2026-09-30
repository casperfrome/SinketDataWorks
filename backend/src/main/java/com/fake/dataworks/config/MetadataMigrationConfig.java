package com.fake.dataworks.config;

import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class MetadataMigrationConfig {
    @Bean
    FlywayMigrationStrategy metadataMigrations(@Value("${studio.migration.baseline-existing:false}") boolean baseline) {
        return flyway -> {
            JdbcTemplate jdbc = new JdbcTemplate(flyway.getConfiguration().getDataSource());
            Set<String> tables = new HashSet<>(jdbc.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE()", String.class));
            if (!tables.isEmpty() && !tables.contains("flyway_schema_history")) {
                if (!baseline) throw new IllegalStateException("Existing metadata requires backup and explicit STUDIO_BASELINE_EXISTING=true for its first migration.");
                Map<String,String> columns = Map.of(
                    "dw_workspace", "id,name,code,region",
                    "dw_object", "id,workspace_id,parent_id,kind,node_type,name,description,content,config_json,tags_json,favorite,deleted,version,owner,updated_at,deletion_group,parent_key,live_key",
                    "dw_version", "id,object_id,version,name,content,config_json,created_at",
                    "dw_run", "id,workspace_id,object_id,status,data_json,snapshot_json,created_at",
                    "dw_record", "id,workspace_id,kind,data_json,created_at",
                    "dw_preference", "id,data_json",
                    "dw_file", "object_id,storage_name,original_name,content_type,file_size");
                if (!tables.equals(columns.keySet())) throw new IllegalStateException("Unexpected metadata tables; baseline refused.");
                columns.forEach((table,expected) -> {
                    Set<String> actual = new HashSet<>(jdbc.queryForList("SELECT column_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=?",String.class,table));
                    if (!actual.equals(Set.of(expected.split(",")))) throw new IllegalStateException("Metadata schema differs from V1: " + table);
                });
                flyway.baseline();
            }
            flyway.migrate();
        };
    }
}
