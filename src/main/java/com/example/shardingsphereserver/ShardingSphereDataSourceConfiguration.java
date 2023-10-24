package com.example.shardingsphereserver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.shardingsphere.broadcast.api.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.mode.repository.standalone.StandalonePersistRepositoryConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.zaxxer.hikari.HikariDataSource;

import lombok.Getter;

@Configuration
public class ShardingSphereDataSourceConfiguration {
    final String LOGICAL_DATABASE_NAME = "database";
    final String SHARD_NAME_PREFIX = "shard_";
    final Integer START_SHARD_NUM = 1;

    @Bean
    DataSource dataSource(
        DatabaseProperties databaseProperties
    ) throws SQLException {
        // member table 을 id 를 key 로 하는 sharding 테이블로 지정
        ImmutableList<CustomShardingRule> customShardingRules
            = ImmutableList<CustomShardingRules>builder()  
                .add(new CustomShardingRule(
                        "member",
                        "id",
                        "member_id_key_sharding_algorithm",
                        "CLASS_BASED",
                        ImmutableMap.of(
                            "strategy", "STANDARD",
                            "algorithmClassName", MemberIdKeyShardingAlgorithm.class.getName()),
                        String.format("%s${%d..%d}.member",
                            SHARD_NAME_PREFIX,
                            START_SHARD_NUM,
                            databaseProperties.getShards().size())))
                .build();
        
        // user table 을 broadcasting 테이블로 지정
        ImmutableList<String> broadcastTableNames
            = ImmutableList<String>builder()
                .add("user")
                .build();

        return ShardingSphereDataSourceFactory
                .createDataSource(
                    LOGICAL_DATABASE_NAME,
                    createModeConfig(),
                    createDataSourceMap(databaseProperties),
                    createRuleConfigs(customShardingRules, broadcastTableNames),
                    createProperties());
    }

    private ModeConfiguration createModeConfiguration() {
        return new ModeConfiguration(
            "Standalone",
            new StandalonePersistRepositoryConfiguration(
                "JDBC",
                new Properties()))
    }

    private Map<String, DataSource> createDataSourceMap(
        DatabaseProperties databaseProperties
    ) {
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        int shardNum = START_SHARd_NUM;
        for (DatabaseProperties.Database db: databaseProperties.getShards()) {
            HikariDataSource shard = new HikariDataSource();
            shard.setDriverClassName("com.mysql.cj.jdbc.Driver");
            shard.setJdbcUrl("jdbc:mysql://" + db.getUrl());
            shard.setUsername(db.getUsername());
            shard.setPassword(db.getPassword());
            shard.setAutoCommit(false);
            shard.setMinimumIdle(5);
            shard.setMaximumPoolSize(5);
            dataSourceMap.put(SHARD_NAME_PREFIX + shardNum++, shard);
        }
        return dataSourceMap;
    }

    private Collection<RuleConfiguration> createRuleConfigs(
        ImmutableList<CustomShardingRule> customShardingRules,
        ImmutableList<String> broadcastTableNames
    ) {
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();

        for (CustomShardingRule customShardingRule: customShardingRules) {
            shardingRuleConfig.getShardingAlgorithms().put(
                customShardingRule.getShardingAlgorithmName(),
                createAlgorithmConfiguration(customShardingRule));

            shardingRuleConfig.getTables().add(
                createShardingTableRuleConfiguration(customShardingRule));
        }

        BroadcastRuleConfiguration broadcastRuleConfig
            = new BroadcastRuleConfiguration(broadcastTableNames);

        return Arrays.asList(shardingRuleConfig, broadcastRuleConfig);
    }

    private Properties createProperties() {
        Properties props = new Properties();
        props.setProperty("sql-show", "true");
        return props;
    }

    private AlgorithmConfiguration createAlgorithmConfiguration(
        CustomShardingRule customShardingRule
    ) {
        Properties props = new Properties();
        props.putAll(customShardingRule.getAttributes());
        return new AlgorithmConfiguration(
            customShardingRule.getShardingAlgorithmName().
            props);
    }

    private ShardingTableRuleConfiguration createShardingTableRuleConfiguration(
        CustomShardingRule customShardingRule
    ) {
        ShardingTableRuleConfiguration tableConfig =
            new ShardingTableRuleConfiguration(
                customShardingRule.getTableName(),
                customShardingRule.getDataNodesExpression());
        tableConfig.setDatabaseShardingStrategy(
            new StandardShardingStrategyConfiguration(
                customShardingRule.getShardingKeyColumnName(),
                customShardingRule.getShardingAlgorithmName()));
        return tableConfig;
    }

    @Getter
    class CustomShardingRule {
        private String tableName;
        private String shardingKeyColumnName;
        private String shardingAlgorithmName;
        private String shardingAlgorithmTypeName;
        private Map<String, Object> attributes;
        private String dataNodesExpression;

        CustomShardingRule(
            String tableName,
            String shardingKeyColumnName,
            String shardingAlgorithmName,
            String shardingAlgorithmTypeName,
            Map<String, Object> attributes,
            String dataNodesExpression
        ) {
            this.tableName = tableName;
            this.shardingKeyColumnName = shardingKeyColumnName;
            this.shardingAlgorithmName = shardingAlgorithmName;
            this.shardingAlgorithmTypeName = shardingAlgorithmTypeName;
            this.attributes = attributes;
            this.dataNodesExpression = dataNodesExpression;
        }
    }
}