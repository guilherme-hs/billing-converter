package com.tenacity.billing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ImportResource;
import java.util.List;

@SpringBootApplication
@ImportResource("file:config.xml")
public class BillingConverterApplication {

    private static Logger logger = LoggerFactory.getLogger(BillingConverterApplication.class);

    private static ApplicationContext ctx;

    public static void main(String[] args) {
        ctx = SpringApplication.run(BillingConverterApplication.class, args);
        logger.info("Billing Converter Started...");
        List<RemoteServer> remoteServers = ctx.getBean("remoteServers",List.class);
        logger.info("Servers pooling:");
        for (RemoteServer remoteServer : remoteServers) {
            logger.info(remoteServer.getName()+"("+remoteServer.getAddress()+")");
        }
        logger.info("Stopping Billing Converter...");
    }

}
