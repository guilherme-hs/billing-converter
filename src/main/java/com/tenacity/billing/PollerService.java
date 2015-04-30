package com.tenacity.billing;

import com.tenacity.sopho.domain.SophoCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Keep running and pooling the remote servers for calls
 */
@Service
public class PollerService {

    private boolean stop = true;

    protected int max_registries;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    protected List<RemoteServer> remoteServerList;

    public void run(){
        stop = false;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.error("Error loading Mysql driver", e);
        } finally {

        }
    }

    /**
     * Requests the poller service to stop
     */
    public void stop(){
        stop = true;
    }

    /**
     * Verifies if the service is required to stop
     * @return stop;
     */
    public boolean isStop() {
        return stop;
    }

    protected List<SophoCall> getBilling(Connection connection,String table, int initialId){
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " +
                    table + " where Id > " + initialId + " limit "+max_registries);


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

}
