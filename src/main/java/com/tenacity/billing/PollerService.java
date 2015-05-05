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
import java.text.DecimalFormat;
import java.util.*;

/**
 * Keep running and pooling the remote servers for calls
 */
@Service
public class PollerService {

    public static final String CALLDATE_COLLUMN = "calldate";
    public static final int INITIAL_REFERENCE_NUMBER = 100;
    public static final int MAX_REFERENCE_NUMBER = 199;
    public static final int NO_SECONDS = 0;
    private boolean stop = true;

    protected int max_registries=1000;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected StringBuilder sb = new StringBuilder();

    DecimalFormat referenceNumberFormatter = new DecimalFormat("0000");

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

    protected List<SophoCall> getBilling(
            Connection connection,String table, int initialId, List<Map<String,String>> numberMap){
        logger.info("Connecting on database and fetching records...");
        List<SophoCall> returnValue = new ArrayList<>();
        int referenceNumber = INITIAL_REFERENCE_NUMBER;
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " +
                    table + " where Id > " + initialId + " limit "+max_registries);
            while(resultSet.next()){
                SophoCall sophoCall = new SophoCall();
                //gets the date
                Date date = resultSet.getTimestamp(CALLDATE_COLLUMN);
                date.setSeconds(NO_SECONDS);
                sophoCall.setDate(date);
                if(referenceNumber > MAX_REFERENCE_NUMBER){
                    referenceNumber = INITIAL_REFERENCE_NUMBER;
                }
                sophoCall.setReferenceNumber(referenceNumberFormatter.format(referenceNumber));
                returnValue.add(sophoCall);
            }
            return returnValue;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

}
