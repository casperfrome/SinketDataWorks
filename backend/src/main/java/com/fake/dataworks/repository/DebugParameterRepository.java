package com.fake.dataworks.repository;

import com.fake.dataworks.config.JsonCodec;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Node debugging preferences are independent of code versions and release snapshots. */
@Repository
public class DebugParameterRepository {
    private final JdbcTemplate jdbc;
    private final JsonCodec json;
    public DebugParameterRepository(JdbcTemplate jdbc,JsonCodec json) {this.jdbc=jdbc;this.json=json;}
    public Map<String,Object> load(String objectId) {
        return jdbc.query("SELECT parameters_json FROM dw_node_debug_parameters WHERE object_id=?",(r,n)->json.map(r.getString(1)),objectId).stream().findFirst().orElse(Map.of());
    }
    public void save(String objectId,Map<String,String> parameters,String updatedAt) {
        if(parameters.isEmpty()) {jdbc.update("DELETE FROM dw_node_debug_parameters WHERE object_id=?",objectId);return;}
        String value=json.write(parameters);
        jdbc.update("INSERT INTO dw_node_debug_parameters(object_id,parameters_json,updated_at) VALUES(?,?,?) ON DUPLICATE KEY UPDATE parameters_json=?,updated_at=?",objectId,value,updatedAt,value,updatedAt);
    }
}
