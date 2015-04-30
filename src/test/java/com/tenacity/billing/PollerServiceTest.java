package com.tenacity.billing;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BillingConverterApplication.class)
public class PollerServiceTest {

    public static final String DEFAULT_CDR_TABLE = "asteriskcdrdb";
    public static final int INITIAL_ID = 0;
    @Autowired
    protected PollerService pollerService;

    protected Connection connection;
    protected Statement statement;

    @BeforeClass
    public static void setupClass() throws Exception{
        Class.forName("com.mysql.jdbc.Driver");
        // Setup the connection with the DB
        Connection connection = DriverManager
                .getConnection("jdbc:mysql://localhost?user=root&password=advah310755");
        Statement statement = connection.createStatement();
        statement.execute("DROP DATABASE IF EXISTS asteriskcdrdbTest");
        statement.execute("CREATE DATABASE asteriskcdrdbTest");
        connection.close();

    }

    @Before
    public void setUp() throws Exception{
        connection = DriverManager
                .getConnection("jdbc:mysql://localhost/asteriskcdrdbTest?user=root&password=advah310755");
        statement = connection.createStatement();
        statement.execute("DROP  TABLE IF EXISTS cdr");
        statement.execute("CREATE TABLE `cdr` (" +
                "`Id` int(11) NOT NULL AUTO_INCREMENT," +
                "`calldate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00'," +
                "`clid` varchar(80) NOT NULL DEFAULT ''," +
                "`src` varchar(80) NOT NULL DEFAULT ''," +
                "`dst` varchar(80) NOT NULL DEFAULT ''," +
                "`dcontext` varchar(80) NOT NULL DEFAULT ''," +
                "`channel` varchar(80) NOT NULL DEFAULT ''," +
                "`dstchannel` varchar(80) NOT NULL DEFAULT ''," +
                "`lastapp` varchar(80) NOT NULL DEFAULT ''," +
                "`lastdata` varchar(80) NOT NULL DEFAULT ''," +
                "`duration` int(11) NOT NULL DEFAULT '0'," +
                "`billsec` int(11) NOT NULL DEFAULT '0'," +
                "`disposition` varchar(45) NOT NULL DEFAULT ''," +
                "`amaflags` int(11) NOT NULL DEFAULT '0'," +
                "`accountcode` varchar(20) NOT NULL DEFAULT ''," +
                "`uniqueid` varchar(32) NOT NULL DEFAULT ''," +
                "`userfield` varchar(255) NOT NULL DEFAULT ''," +
                "`port_op` varchar(20) DEFAULT NULL," +
                "`port_type` varchar(20) DEFAULT NULL, " +
                "PRIMARY KEY (`Id`)," +
                "KEY `calldate` (`calldate`), " +
                "KEY `dst` (`dst`), " +
                "KEY `accountcode` (`accountcode`), " +
                "KEY `uniqueidindex` (`uniqueid`)" +
                ") ENGINE=MyISAM DEFAULT CHARSET=latin1;");
        statement.execute("INSERT INTO `cdr` VALUES " +
                "(1671,'2015-04-29 08:26:03','6111','6111','035229540','OUT_LOCAL2','SIP/6111-000009ba','Khomp/B0C0-0.0','Hangup','',2,0,'FAILED',3,'22401','1430306763.3223','\\\"B\\\"',NULL,NULL)," +
                "(1672,'2015-04-29 08:26:19','6110','6110','4377','OUT_LOCAL','SIP/6110-000009bb','SIP/288-000009bc','Dial','Sip/288/4377,90,tT',33,0,'NO ANSWER',3,'','1430306779.3225','',NULL,NULL)," +
                "(1673,'2015-04-29 08:29:15','6110','6110','4377','OUT_LOCAL','SIP/6110-000009bd','SIP/288-000009be','Dial','Sip/288/4377,90,tT',18,0,'NO ANSWER',3,'','1430306955.3227','',NULL,NULL)," +
                "(1674,'2015-04-29 08:34:48','6108','6108','4376','OUT_LOCAL','SIP/6108-000009bf','SIP/288-000009c0','Dial','Sip/288/4376,90,tT',4,0,'BUSY',3,'','1430307288.3229','',NULL,NULL)," +
                "(1675,'2015-04-29 08:34:58','6108','6108','4376','OUT_LOCAL','SIP/6108-000009c1','SIP/288-000009c2','Dial','Sip/288/4376,90,tT',4,0,'BUSY',3,'','1430307298.3231','',NULL,NULL)," +
                "(1676,'2015-04-29 08:35:34','6108','6108','4376','OUT_LOCAL','SIP/6108-000009c4','SIP/288-000009c5','Dial','Sip/288/4376,90,tT',4,0,'BUSY',3,'','1430307334.3235','',NULL,NULL)," +
                "(1677,'2015-04-29 08:35:11','6111','6111','030259500','OUT_LOCAL2','SIP/6111-000009c3','Khomp/B0C0-0.0','Dial','Khomp/b0l0/30259500,90,tT',83,68,'ANSWERED',3,'22401','1430307311.3233','\\\"B\\\"',NULL,NULL)," +
                "(1678,'2015-04-29 08:36:49','6111','6111','035229540','OUT_LOCAL2','SIP/6111-000009c6','Khomp/B0C0-0.0','Hangup','',3,0,'FAILED',3,'22401','1430307409.3237','\\\"B\\\"',NULL,NULL)," +
                "(1679,'2015-04-29 08:39:56','6108','6108','4376','OUT_LOCAL','SIP/6108-000009c7','SIP/288-000009c8','Dial','Sip/288/4376,90,tT',129,120,'ANSWERED',3,'','1430307596.3239','',NULL,NULL)," +
                "(1680,'2015-04-29 09:03:45','6109','6109','4272','INT_INTERNO','SIP/6109-000009c9','SIP/288-000009ca','Dial','Sip/288/4272,90,tT',36,30,'ANSWERED',3,'','1430309025.3241','',NULL,NULL)," +
                "(1681,'2015-04-29 09:04:25','6109','6109','4240','INT_INTERNO','SIP/6109-000009cb','SIP/288-000009cc','Dial','Sip/288/4240,90,tT',354,337,'ANSWERED',3,'','1430309065.3243','',NULL,NULL)," +
                "(1682,'2015-04-29 09:10:01','3134755794','3134755794','6108','khomp-00-002','Khomp/B0C4-0.0','SIP/6108-000009cd','Dial','sip/6108,90,tT',18,14,'ANSWERED',3,'','1430309401.3245','',NULL,NULL)," +
                "(1683,'2015-04-29 09:10:36','3134755794','3134755794','6108','khomp-00-002','Khomp/B0C5-0.0','SIP/6108-000009ce','Dial','sip/6108,90,tT',111,107,'ANSWERED',3,'','1430309436.3247','',NULL,NULL)," +
                "(1684,'2015-04-29 09:11:52','6110','6110','4377','OUT_LOCAL','SIP/6110-000009cf','SIP/288-000009d0','Dial','Sip/288/4377,90,tT',173,162,'ANSWERED',3,'','1430309512.3249','',NULL,NULL)," +
                "(1685,'2015-04-29 09:15:17','6109','6109','4240','INT_INTERNO','SIP/6109-000009d1','SIP/288-000009d2','Dial','Sip/288/4240,90,tT',158,146,'ANSWERED',3,'','1430309717.3251','',NULL,NULL)," +
                "(1686,'2015-04-29 09:19:37','\\\"4377\\\" <288>','288','6110','INT_INTERNO2','SIP/288-000009d3','SIP/6108-000009d5','Dial','sip/6110,90,tT',61,20,'ANSWERED',3,'','1430309977.3253','',NULL,NULL)," +
                "(1687,'2015-04-29 09:22:33','6110','6110','4377','OUT_LOCAL','SIP/6110-000009d6','SIP/288-000009d7','Dial','Sip/288/4377,90,tT',13,9,'ANSWERED',3,'','1430310153.3256','',NULL,NULL)," +
                "(1688,'2015-04-29 09:22:54','6110','6110','4377','OUT_LOCAL','SIP/6110-000009d8','SIP/288-000009d9','Dial','Sip/288/4377,90,tT',4,0,'BUSY',3,'','1430310174.3258','',NULL,NULL)," +
                "(1689,'2015-04-29 09:24:16','6110','6110','4377','OUT_LOCAL','SIP/6110-000009da','SIP/288-000009db','Dial','Sip/288/4377,90,tT',83,75,'ANSWERED',3,'','1430310256.3260','',NULL,NULL)," +
                "(1690,'2015-04-29 09:27:08','6108','6108','035229457','OUT_LOCAL2','SIP/6108-000009dc','Khomp/B0C0-0.0','Dial','Khomp/b0l0/35229457,90,tT',87,80,'ANSWERED',3,'27206','1430310428.3262','\\\"B\\\"',NULL,NULL)," +
                "(1691,'2015-04-29 09:30:51','6108','6108','036291150','OUT_LOCAL2','SIP/6108-000009dd','Khomp/B0C0-0.0','Dial','Khomp/b0l0/36291150,90,tT',115,94,'ANSWERED',3,'27206','1430310651.3264','\\\"B\\\"',NULL,NULL)," +
                "(1692,'2015-04-29 09:34:49','6109','6109','4216','INT_INTERNO','SIP/6109-000009de','SIP/288-000009df','Dial','Sip/288/4216,90,tT',4,0,'BUSY',3,'','1430310889.3266','',NULL,NULL)," +
                "(1693,'2015-04-29 09:36:08','6109','6109','4216','INT_INTERNO','SIP/6109-000009e0','SIP/288-000009e1','Dial','Sip/288/4216,90,tT',2,0,'NO ANSWER',3,'','1430310968.3268','',NULL,NULL)," +
                "(1747,'2015-04-29 15:48:29','6108','6108','6111','OUT_LOCAL2','SIP/6108-00000a2c','SIP/6111-00000a2d','Dial','sip/6111,90,tT',17,0,'NO ANSWER',3,'','1430333309.3371','',NULL,NULL)," +
                "(1748,'2015-04-29 15:49:29','6111','6111','6109','OUT_LOCAL2','SIP/6111-00000a2e','SIP/6109-00000a2f','Dial','sip/6109,90,tT',23,21,'ANSWERED',3,'','1430333369.3373','',NULL,NULL)," +
                "(1749,'2015-04-29 15:50:05','6115','6115','6108','OUT_DDD2','SIP/6115-00000a30','SIP/6108-00000a31','Dial','sip/6108,90,tT',68,63,'ANSWERED',3,'','1430333405.3375','',NULL,NULL)," +
                "(1750,'2015-04-29 15:51:27','6115','6115','033531535','OUT_LOCAL2','SIP/6115-00000a32','Khomp/B0C0-0.0','Dial','Khomp/b0l0/33531535,90,tT',119,109,'ANSWERED',3,'22401','1430333487.3377','\\\"B\\\"',NULL,NULL)," +
                "(1751,'2015-04-29 16:02:58','\\\"Cabine\\\" <6125>','6125','6120','INT_INTERNO2','SIP/6125-00000a33','SIP/6120-00000a34','Dial','sip/6120,90,tT',20,0,'NO ANSWER',3,'','1430334178.3379','',NULL,NULL)," +
                "(1752,'2015-04-29 16:06:50','6110','6110','4163','OUT_LOCAL','SIP/6110-00000a35','SIP/288-00000a36','Dial','Sip/288/4163,90,tT',4,0,'BUSY',3,'','1430334410.3381','',NULL,NULL)," +
                "(1753,'2015-04-29 16:07:20','\\\"Cabine\\\" <6125>','6125','6118','INT_INTERNO2','SIP/6125-00000a37','SIP/6118-00000a38','Dial','sip/6118,90,tT',7,0,'NO ANSWER',3,'','1430334440.3383','',NULL,NULL)," +
                "(1754,'2015-04-29 16:07:31','\\\"Cabine\\\" <6125>','6125','6120','INT_INTERNO2','SIP/6125-00000a3b','SIP/6120-00000a3c','Dial','sip/6120,90,tT',17,0,'NO ANSWER',3,'','1430334451.3387','',NULL,NULL)," +
                "(1755,'2015-04-29 16:07:21','6110','6110','4245','OUT_LOCAL','SIP/6110-00000a39','SIP/288-00000a3a','Dial','Sip/288/4245,90,tT',41,0,'NO ANSWER',3,'','1430334441.3385','',NULL,NULL)," +
                "(1756,'2015-04-29 16:17:47','6111','6111','6125','OUT_LOCAL2','SIP/6111-00000a3e','SIP/6125-00000a3f','Dial','sip/6125,90,tT',54,48,'ANSWERED',3,'','1430335067.3390','',NULL,NULL)," +
                "(1757,'2015-04-29 16:18:50','6110','6110','4245','OUT_LOCAL','SIP/6110-00000a40','SIP/288-00000a41','Dial','Sip/288/4245,90,tT',61,45,'ANSWERED',3,'','1430335130.3392','',NULL,NULL)," +
                "(1758,'2015-04-29 16:21:19','6108','6108','6111','OUT_LOCAL2','SIP/6108-00000a42','SIP/6111-00000a43','Dial','sip/6111,90,tT',27,26,'ANSWERED',3,'','1430335279.3394','',NULL,NULL)," +
                "(1759,'2015-04-29 16:22:03','6111','6111','035311282','OUT_DDD2','SIP/6111-00000a44','Khomp/B0C0-0.0','Dial','Khomp/b0l0/35311282,90,tT',89,80,'ANSWERED',3,'22037','1430335323.3396','\\\"B\\\"',NULL,NULL)," +
                "(1760,'2015-04-29 16:26:00','6111','6111','02672217','OUT_DDD2','SIP/6111-00000a45','Khomp/B0C0-0.0','Dial','Khomp/b0l0/2672217,90,tT',6,5,'ANSWERED',3,'22037','1430335560.3398','\\\"B\\\"',NULL,NULL)," +
                "(1761,'2015-04-29 16:26:18','6111','6111','025672217','OUT_DDD2','SIP/6111-00000a46','Khomp/B0C0-0.0','Dial','Khomp/b0l0/25672217,90,tT',49,40,'ANSWERED',3,'22037','1430335578.3400','\\\"B\\\"',NULL,NULL)," +
                "(1762,'2015-04-29 16:27:50','6111','6111','035313700','OUT_DDD2','SIP/6111-00000a48','Khomp/B0C0-0.0','Dial','Khomp/b0l0/35313700,90,tT',141,81,'ANSWERED',3,'22037','1430335670.3403','\\\"B\\\"',NULL,NULL)," +
                "(1763,'2015-04-29 16:28:29','6108','6108','4192','OUT_LOCAL','SIP/6108-00000a49','SIP/288-00000a4a','Dial','Sip/288/4192,90,tT',122,110,'ANSWERED',3,'','1430335709.3405','',NULL,NULL)," +
                "(1764,'2015-04-29 16:30:23','6111','6111','035941040','OUT_DDD2','SIP/6111-00000a4b','Khomp/B0C0-0.0','Dial','Khomp/b0l0/35941040,90,tT',51,0,'NO ANSWER',3,'22037','1430335823.3407','\\\"B\\\"',NULL,NULL)," +
                "(1765,'2015-04-29 16:31:26','6111','6111','035941040','OUT_DDD2','SIP/6111-00000a4c','Khomp/B0C0-0.0','Dial','Khomp/b0l0/35941040,90,tT',160,157,'ANSWERED',3,'22037','1430335886.3409','\\\"B\\\"',NULL,NULL)," +
                "(1766,'2015-04-29 18:22:10','3197761981','3197761981','6109','noturno','Khomp/B0C5-0.0','SIP/6109-00000a4d','Dial','sip/6109,90,tT',63,49,'ANSWERED',3,'','1430342530.3411','',NULL,NULL)," +
                "(1767,'2015-04-29 21:10:44','1121760010','1121760010','6114','noturno','Khomp/B0C6-0.0','SIP/6114-00000a4e','Dial','sip/6114,90,tT',21,0,'NO ANSWER',3,'','1430352644.3413','',NULL,NULL)," +
                "(1768,'2015-04-29 21:23:23','3197162577','3197162577','6125','noturno','Khomp/B0C7-0.0','SIP/6125-00000a54','Hangup','',59,0,'NO ANSWER',3,'','1430353403.3420','',NULL,NULL)," +
                "(1769,'2015-04-30 08:14:50','3135962198','3135962198','6105','khomp-00-002','Khomp/B0C8-0.0','SIP/6105-00000a56','Dial','sip/6105,90,tT',21,0,'NO ANSWER',3,'','1430392490.3423','',NULL,NULL)," +
                "(1770,'2015-04-30 08:15:32','3135962198','3135962198','6105','khomp-00-002','Khomp/B0C9-0.0','SIP/6105-00000a57','Dial','sip/6105,90,tT',5,0,'NO ANSWER',3,'','1430392532.3425','',NULL,NULL)," +
                "(1771,'2015-04-30 08:16:22','3187970349','3187970349','6105','khomp-00-002','Khomp/B0C0-0.0','SIP/6105-00000a58','Dial','sip/6105,90,tT',78,53,'ANSWERED',3,'','1430392582.3427','',NULL,NULL)," +
                "(1772,'2015-04-30 08:26:04','\\\"4238\\\" <288>','288','6108','INT_INTERNO2','SIP/288-00000a5a','SIP/6108-00000a5b','Dial','sip/6108,90,tT',93,81,'ANSWERED',3,'','1430393164.3430','',NULL,NULL)," +
                "(1773,'2015-04-30 08:37:45','6108','6108','4238','OUT_LOCAL','SIP/6108-00000a5c','SIP/288-00000a5d','Dial','Sip/288/4238,90,tT',211,203,'ANSWERED',3,'','1430393865.3432','',NULL,NULL)," +
                "(1774,'2015-04-30 08:50:15','6111','6111','4109','OUT_LOCAL','SIP/6111-00000a5f','SIP/288-00000a60','Dial','Sip/288/4109,90,tT',38,0,'NO ANSWER',3,'','1430394615.3436','',NULL,NULL)," +
                "(1775,'2015-04-30 08:51:02','6111','6111','4109','OUT_LOCAL','SIP/6111-00000a62','SIP/288-00000a63','Dial','Sip/288/4109,90,tT',4,0,'BUSY',3,'','1430394662.3439','',NULL,NULL)," +
                "(1776,'2015-04-30 08:51:12','6111','6111','4109','OUT_LOCAL','SIP/6111-00000a64','SIP/288-00000a65','Dial','Sip/288/4109,90,tT',4,0,'BUSY',3,'','1430394672.3441','',NULL,NULL)," +
                "(1777,'2015-04-30 08:51:24','6111','6111','4162','OUT_LOCAL','SIP/6111-00000a66','SIP/288-00000a67','Dial','Sip/288/4162,90,tT',4,0,'BUSY',3,'','1430394684.3443','',NULL,NULL)," +
                "(1778,'2015-04-30 08:51:34','6111','6111','4162','OUT_LOCAL','SIP/6111-00000a68','SIP/288-00000a69','Dial','Sip/288/4162,90,tT',4,0,'BUSY',3,'','1430394694.3445','',NULL,NULL)," +
                "(1779,'2015-04-30 08:51:41','6111','6111','4109','OUT_LOCAL','SIP/6111-00000a6a','SIP/288-00000a6b','Dial','Sip/288/4109,90,tT',4,0,'BUSY',3,'','1430394701.3447','',NULL,NULL)," +
                "(1780,'2015-04-30 08:51:49','6111','6111','4105','OUT_LOCAL','SIP/6111-00000a6c','SIP/288-00000a6d','Dial','Sip/288/4105,90,tT',4,0,'BUSY',3,'','1430394709.3449','',NULL,NULL)," +
                "(1781,'2015-04-30 08:52:40','6111','6111','035298000','OUT_DDD2','SIP/6111-00000a70','Khomp/B0C1-0.0','Dial','Khomp/b0l0/35298000,90,tT',141,130,'ANSWERED',3,'22037','1430394760.3453','\\\"B\\\"',NULL,NULL)," +
                "(1782,'2015-04-30 08:52:15','6108','6108','4371','OUT_LOCAL','SIP/6108-00000a6e','SIP/288-00000a6f','Dial','Sip/288/4371,90,tT',285,274,'ANSWERED',3,'','1430394735.3451','',NULL,NULL)," +
                "(1783,'2015-04-30 08:45:27','6102','6102','033060217','OUT_LOCAL2','SIP/6102-00000a5e','Khomp/B0C0-0.0','Dial','Khomp/b0l0/33060217,90,tT',715,684,'ANSWERED',3,'21418','1430394327.3434','\\\"B\\\"',NULL,NULL)");
    }


    @Test
    public void stopTest(){
        assertThat(pollerService.isStop(), equalTo(true));
        pollerService.run();
        assertThat(pollerService.isStop(), equalTo(false));
        pollerService.stop();
        assertThat(pollerService.isStop(), equalTo(true));
    }

    @Test
    public void getBillingTest() throws Exception{


    }
}