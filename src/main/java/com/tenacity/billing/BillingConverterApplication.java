package com.tenacity.billing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ImportResource;

import java.rmi.Remote;

@SpringBootApplication
@ImportResource("file:config.xml")
public class BillingConverterApplication {

    private static Logger logger = LoggerFactory.getLogger(BillingConverterApplication.class);

    private static ApplicationContext ctx;

    public static void main(String[] args) {
        ctx = SpringApplication.run(BillingConverterApplication.class, args);
        logger.info("Billing Converter Started...");
        RemoteServer server1 = ctx.getBean("server1",RemoteServer.class);
        logger.info("Stopping Billing Converter...");
    }

}
