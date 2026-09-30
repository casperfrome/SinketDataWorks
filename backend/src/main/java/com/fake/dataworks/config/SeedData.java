package com.fake.dataworks.config;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Bootstrap workspace identities only. User files and connections are never seeded. */
@Component
public class SeedData implements ApplicationRunner {
    private final JdbcTemplate jdbc;
    public SeedData(JdbcTemplate jdbc) {this.jdbc=jdbc;}
    @Override public void run(ApplicationArguments args) {
        jdbc.update("INSERT IGNORE INTO dw_workspace(id,name,code,region) VALUES('local-workspace','数据开发工作空间','dataworks_local','华北2（北京）'),('sandbox','沙箱工作空间','dataworks_sandbox','华东1（杭州）')");
    }
}
