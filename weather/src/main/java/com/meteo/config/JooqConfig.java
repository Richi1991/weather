package com.meteo.config;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import javax.sql.DataSource;

@Configuration
public class JooqConfig {


    public DataSourceConnectionProvider connectionProvider(DataSource dataSource) {
        return new DataSourceConnectionProvider(new TransactionAwareDataSourceProxy(dataSource));
    }

    @Bean
    public DSLContext dsl(DataSourceConnectionProvider connectionProvider) {
        DefaultConfiguration config = new DefaultConfiguration();
        config.set(connectionProvider);
        config.set(SQLDialect.POSTGRES); // Importante para que funcione el DISTINCT ON
        return new DefaultDSLContext(config);
    }
}
