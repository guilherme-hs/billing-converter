package com.tenacity.billing;

import com.tenacity.sopho.domain.*;
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
public class PollerServiceImpl {

    public static final String CALLDATE_COLLUMN = "calldate";
    public static final int INITIAL_REFERENCE_NUMBER = 100;
    public static final int MAX_REFERENCE_NUMBER = 199;
    public static final int NO_SECONDS = 0;
    public static final int DEFAULT_IBSC = 14;
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

    /**
     * Gets teh calls from the provided connection
     * @param connection
     * @param table
     * @param initialId
     * @param numberMap
     * @return
     */
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
                                logger.info("Userfield:"+userfield);
                                if(userfield.equalsIgnoreCase("\"P\"")){
                                    logger.info("Setting pivate call...");
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

}
