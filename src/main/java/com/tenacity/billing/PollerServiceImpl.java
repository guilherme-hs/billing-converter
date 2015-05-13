package com.tenacity.billing;

import com.tenacity.sopho.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Date;

/**
 * Keep running and pooling the remote servers for calls
 */
@Service
public class PollerServiceImpl implements PollerService{

    public static final String CALLDATE_COLLUMN = "calldate";
    public static final int INITIAL_REFERENCE_NUMBER = 100;
    public static final int MAX_REFERENCE_NUMBER = 199;
    public static final int NO_SECONDS = 0;
    public static final int DEFAULT_IBSC = 14;
    public static final int NO_REGISTERS = 0;
    public static final String DEFAULT_SYNC_FILE = "sync-status.properties";
    public static final String VERSION_PROPERTY = "version";
    public static final String PROP_VERSION = "1.0";
    private boolean stop = true;

    protected int max_registries=1000;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected StringBuilder sb = new StringBuilder();

    protected DecimalFormat referenceNumberFormatter = new DecimalFormat("0000");

    protected int referenceNumber = INITIAL_REFERENCE_NUMBER;

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

        for (RemoteServer remoteServer : remoteServerList) {
            pollServer(remoteServer);
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

    /**
     * Gets teh calls from the provided connection
     * @param connection - connection provided
     * @param table - table with the cdr records
     * @param initialId
     * @param numberMap
     * @return
     */
    protected List<SophoCall> getBilling(
            Connection connection,String table, int initialId, List<Map<String,String>> numberMap){
        logger.info("Connecting on database and fetching records...");
        List<SophoCall> returnValue = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " +
                    table + " where Id > " + initialId + " limit "+max_registries);
            while(resultSet.next()){
                SophoCall sophoCall = new SophoCall();

                int id = resultSet.getInt("Id");
                sophoCall.setId(""+id);
                //gets the date
                Date date = resultSet.getTimestamp(CALLDATE_COLLUMN);
                date.setSeconds(NO_SECONDS);
                sophoCall.setDate(date);
                if(referenceNumber > MAX_REFERENCE_NUMBER){
                    referenceNumber = INITIAL_REFERENCE_NUMBER;
                }
                sophoCall.setReferenceNumber(referenceNumberFormatter.format(referenceNumber));


                boolean srcRegexFound = false;
                //tests the type of the src
                String sophoCallSrc = resultSet.getString("src");
                for (Map<String, String> rules : numberMap) {
                    if(!srcRegexFound){
                        if(sophoCallSrc.matches(rules.get("regex"))){
                            sophoCall.setPartyAtype(SophoPartyType.valueOf(rules.get("type")));
                            if(rules.get("replacement")!=null){
                                sophoCall.setPartyAFarEnd(
                                        sophoCallSrc.replaceAll(rules.get("regex"),rules.get("replacement")));
                            }else{
                                sophoCall.setPartyAFarEnd(sophoCallSrc);
                            }
                            if(sophoCall.getPartyAtype().equals(SophoPartyType.PSTN)){
                                sophoCall.setPartyAFarEnd(sophoCallSrc);
                                if(rules.get("route") == null){
                                    sophoCall.setPartyARoute("000");
                                }else{
                                    sophoCall.setPartyARoute(rules.get("route"));
                                }
                                if(rules.get("line") == null){
                                    sophoCall.setPartyALine("0001");
                                }else{
                                    sophoCall.setPartyALine(rules.get("line"));
                                }
                            }

                        }
                    }
                }

                boolean dstRegexFound = false;
                //tests the type of the src
                String sophoCallDst = resultSet.getString("dst");
                String userfield;
                for (Map<String, String> rules : numberMap) {
                    if(!dstRegexFound){
                        if(sophoCallDst.matches(rules.get("regex"))){
                            sophoCall.setPartyBtype(SophoPartyType.valueOf(rules.get("type")));
                            if(rules.get("replacement")!=null){
                                sophoCall.setPartyBFarEnd(
                                        sophoCallDst.replaceAll(rules.get("regex"), rules.get("replacement")));
                            }else{
                                sophoCall.setPartyBFarEnd(sophoCallDst);
                            }
                            if(sophoCall.getPartyBtype().equals(SophoPartyType.PSTN)){
                                sophoCall.setDestination(sophoCall.getPartyBFarEnd());
                                sophoCall.setPartyBFarEnd("");
                                if(rules.get("route") == null){
                                    sophoCall.setPartyBRoute("000");
                                }else{
                                    sophoCall.setPartyBRoute(rules.get("route"));
                                }
                                if(rules.get("line") == null){
                                    sophoCall.setPartyBLine("0001");
                                }else{
                                    sophoCall.setPartyBLine(rules.get("line"));
                                }
                                userfield = resultSet.getString("userfield");
                                if(userfield.equalsIgnoreCase("\"P\"")){
                                    sophoCall.setPrivateCall(true);
                                }

                            }
                        }
                    }
                }

                sophoCall.setIbsc(DEFAULT_IBSC);
                int billsec = resultSet.getInt("billsec");
                int duration = resultSet.getInt("duration");

                if(resultSet.getString("disposition").equals("ANSWERED")){
                    sophoCall.setAnsweredStatus(true);
                }
                if(resultSet.getString("disposition").equals("ANSWERED") ||
                        resultSet.getString("disposition").equals("NO ANSWER")){
                    sophoCall.setAnswerDelay(duration-billsec);
                    sophoCall.setCallDuration(duration);
                    sophoCall.setConversationDuration(billsec);
                }

                sophoCall.setAnswerDelayType(SophoAnswerDelayType.BOTH);
                sophoCall.setPasswordIndication(SophoPasswordIndication.NORMAL_CALL);

                String accountCode = resultSet.getString("accountcode");
                if( accountCode!= null && !accountCode.isEmpty()){
                    sophoCall.setCostCentreType(SophoCostCentreType.PID);
                    sophoCall.setCostCentre(resultSet.getString("accountcode"));
                }else{
                    sophoCall.setCostCentreType(SophoCostCentreType.NO_COST_CENTRE);
                }
                returnValue.add(sophoCall);
                referenceNumber += 2;
            }
            return returnValue;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected List<SophoCall> pollServer(RemoteServer remoteServer){
        int actualId=0;
        Connection connection = null;
        Statement statement = null;
        List<SophoCall> sophoCalls = new ArrayList<>();

        //tries to load the actual state
        Properties prop = new Properties();
        InputStream input = null;
        File syncStatusFile = new File(DEFAULT_SYNC_FILE);
        OutputStream output = null;

        try{
            if(syncStatusFile.isFile() && syncStatusFile.canRead())
            input = new FileInputStream(DEFAULT_SYNC_FILE);
            if(input != null){
                prop.load(input);
            }else{
                logger.warn("sync-status file not found... creating one");
                output = new FileOutputStream(DEFAULT_SYNC_FILE);
                prop.setProperty(VERSION_PROPERTY, PROP_VERSION);
                prop.store(output,null);
            }
        }catch (IOException ex){
            logger.error("Error reading actual state..",ex);
        }finally {
            if(input != null ){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(output!=null){
                try {
                    output.flush();
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            syncStatusFile.delete();
            output = new FileOutputStream(DEFAULT_SYNC_FILE);
        } catch (FileNotFoundException e) {
            logger.error("error opening sync-status file...",e);
            return null;
        }

        String savedId = prop.getProperty(remoteServer.getName());
        if(savedId != null){
            actualId = Integer.parseInt(savedId);
        }

        if(stop){
            logger.warn("Asked to stop the polling... Stopping now!!!");
        }
        logger.info("Polling Server:" + remoteServer.getName() + "(" + remoteServer.getAddress() + ")");
        try {
            connection= DriverManager
                    .getConnection("jdbc:mysql://" + remoteServer.getAddress() +":"+remoteServer.getPort()
                            +"/"+remoteServer.getDatabase()+"?user=" + remoteServer.getUsername()
                            + "&password=" + remoteServer.getPassword());
            statement = connection.createStatement();
            List<SophoCall> actualCalls = new ArrayList<>();
            do{
                actualCalls = getBilling(connection,remoteServer.getTable(),actualId,remoteServer.getNumberMap());
                if(actualCalls.size() != NO_REGISTERS) {
                    actualId = Integer.parseInt(actualCalls.get(actualCalls.size() - 1).getId());
                }
                sophoCalls.addAll(actualCalls);
                prop.setProperty(remoteServer.getName(),""+actualId);
                prop.store(output, null);
            }while(actualCalls.size() > 0);
        } catch (SQLException e) {
            logger.
                    error("Error Polling Server " + remoteServer.getName() + "(" + remoteServer.getAddress() + ")", e);
        }catch (IOException e){
            logger.error("Error saving sync-status file...");
        }
        finally {
            if(statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection!=null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(output!=null){
                try {
                    output.flush();
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return sophoCalls;
    }


}
