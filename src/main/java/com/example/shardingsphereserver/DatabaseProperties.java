package com.example.shardingsphereserver;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import lombok.Data;

@Component
@ConfigurationProperties(prefix = "database")
@Data
public class DatabaseProperties {
    private List<Database> shards;

    @Data
    public static class Database {
        private String url;
        private String username;
        private String password;
    }
}
