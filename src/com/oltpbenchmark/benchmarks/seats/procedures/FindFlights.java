/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.seats.procedures;

import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;


import org.apache.log4j.Logger;
import com.oltpbenchmark.api.*;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.benchmarks.seats.util.RestQuery;

public class FindFlights extends Procedure {
    private static final Logger LOG = Logger.getLogger(FindFlights.class);
    
    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
//    private static final VoltTable.ColumnInfo[] RESULT_COLS = {
//        new VoltTable.ColumnInfo("F_ID", VoltType.BIGINT),
//        new VoltTable.ColumnInfo("SEATS_LEFT", VoltType.BIGINT),
//        new VoltTable.ColumnInfo("AL_NAME", VoltType.STRING),
//        new VoltTable.ColumnInfo("DEPART_TIME", VoltType.TIMESTAMP),
//        new VoltTable.ColumnInfo("DEPART_AP_CODE", VoltType.STRING),
//        new VoltTable.ColumnInfo("DEPART_AP_NAME", VoltType.STRING),
//        new VoltTable.ColumnInfo("DEPART_AP_CITY", VoltType.STRING),
//        new VoltTable.ColumnInfo("DEPART_AP_COUNTRY", VoltType.STRING),
//        new VoltTable.ColumnInfo("ARRIVE_TIME", VoltType.TIMESTAMP),
//        new VoltTable.ColumnInfo("ARRIVE_AP_CODE", VoltType.STRING),
//        new VoltTable.ColumnInfo("ARRIVE_AP_NAME", VoltType.STRING),
//        new VoltTable.ColumnInfo("ARRIVE_AP_CITY", VoltType.STRING),
//        new VoltTable.ColumnInfo("ARRIVE_AP_COUNTRY", VoltType.STRING),
//    };
    
    public final SQLStmt GetNearbyAirports = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstants.TABLENAME_AIRPORT_DISTANCE +
            " WHERE D_AP_ID0 = ? " +
            "   AND D_DISTANCE <= ? " +
            " ORDER BY D_DISTANCE ASC "
    );
 
    public final SQLStmt GetAirportInfo = new SQLStmt(
            "SELECT AP_CODE, AP_NAME, AP_CITY, AP_LONGITUDE, AP_LATITUDE, " +
                  " CO_ID, CO_NAME, CO_CODE_2, CO_CODE_3 " +
             " FROM " + SEATSConstants.TABLENAME_AIRPORT + ", " +
                        SEATSConstants.TABLENAME_COUNTRY +
            " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
    );
    
    private final static String BaseGetFlights =
            "SELECT F_ID, F_AL_ID, F_SEATS_LEFT, " +
                  " F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, " +
                  " AL_NAME, AL_IATTR00, AL_IATTR01 " +
             " FROM " + SEATSConstants.TABLENAME_FLIGHT + ", " +
                        SEATSConstants.TABLENAME_AIRLINE +
            " WHERE F_DEPART_AP_ID = ? " +
            "   AND F_DEPART_TIME >= ? AND F_DEPART_TIME <= ? " +
            "   AND F_AL_ID = AL_ID " +
            "   AND F_ARRIVE_AP_ID IN (??)";
    
    public final SQLStmt GetFlights1 = new SQLStmt(BaseGetFlights, 1);
    public final SQLStmt GetFlights2 = new SQLStmt(BaseGetFlights, 2);
    public final SQLStmt GetFlights3 = new SQLStmt(BaseGetFlights, 3);
 
    public List<Object[]> run(Connection conn, long depart_aid, long arrive_aid, Timestamp start_date, Timestamp end_date, long distance, int id) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        assert(start_date.equals(end_date) == false);
        
        final List<Long> arrive_aids = new ArrayList<Long>();
        arrive_aids.add(arrive_aid);
        
        final List<Object[]> finalResults = new ArrayList<Object[]>();
        
        if (distance > 0) {
            // First get the nearby airports for the departure and arrival cities
            StringBuilder sb = new StringBuilder();
            sb.append( "SELECT D_AP_ID0, D_AP_ID1, D_DISTANCE FROM " );
            sb.append( SEATSConstants.TABLENAME_AIRPORT_DISTANCE );
            sb.append( " WHERE D_AP_ID0 = " );
            sb.append( depart_aid );
            sb.append( "   AND D_DISTANCE <= " );
            sb.append( distance );
            sb.append( " ORDER BY D_DISTANCE ASC " );
            List<Map<String, Object>> resultSet = RestQuery.restReadQuery( sb.toString(), id );
            
            String startTs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(start_date);
            String endTs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(end_date);
            for( Map<String,Object> apRow : resultSet ) {
                long aid = new Long( (Integer) apRow.get( "d_ap_id1" ) );
                double aid_distance = (Double) apRow.get( "d_distance" );



                sb = new StringBuilder();
                sb.append( "SELECT F_ID, F_AL_ID, F_SEATS_LEFT, " );
                sb.append( "F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, " );
                sb.append("AL_NAME, AL_IATTR00, AL_IATTR01 FROM " );
                sb.append( SEATSConstants.TABLENAME_FLIGHT ); 
                sb.append( ", " );
                sb.append( SEATSConstants.TABLENAME_AIRLINE );
                sb.append( " WHERE F_DEPART_AP_ID = " );
                sb.append( depart_aid );
                sb.append( " AND F_DEPART_TIME >= '" );
                sb.append( startTs ); //TODO ts format
                sb.append( "' AND F_DEPART_TIME <= '" );
                sb.append( endTs ); // TODO fix ts
                sb.append( "' AND F_AL_ID = AL_ID " );
                sb.append( " AND F_ARRIVE_AP_ID = " );
                sb.append( aid );

                List<Map<String,Object>> flights = RestQuery.restReadQuery( sb.toString(), id );

                for( Map<String,Object> flightRow : flights ) {
                    long f_depart_airport = new Long( (Integer) flightRow.get( "f_depart_ap_id" ) );
                    long f_arrive_airport = new Long( (Integer) flightRow.get( "f_arrive_ap_id" ) );
                    
                    Object row[] = new Object[13];
                    int r = 0;
                    
                    row[r++] = flightRow.get("f_id");    // [00] F_ID
                    row[r++] = flightRow.get("f_seats_left");    // [01] SEATS_LEFT
                    row[r++] = flightRow.get("al_name");  // [02] AL_NAME
                    
                    // DEPARTURE AIRPORT

                    sb = new StringBuilder();
                    sb.append( "SELECT AP_CODE, AP_NAME, AP_CITY, AP_LONGITUDE, AP_LATITUDE, " );
                    sb.append( " CO_ID, CO_NAME, CO_CODE_2, CO_CODE_3 FROM " );
                    sb.append( SEATSConstants.TABLENAME_AIRPORT );
                    sb.append( ", " );
                    sb.append( SEATSConstants.TABLENAME_COUNTRY );
                    sb.append( " WHERE AP_ID = " );
                    sb.append( f_depart_airport );
                    sb.append( " AND AP_CO_ID = CO_ID ");
                    List<Map<String,Object>> aiRows = RestQuery.restReadQuery( sb.toString(), id );
                    Map<String,Object> aiRow = aiRows.get( 0 );
                    row[r++] = flightRow.get("f_depart_time");    // [03] DEPART_TIME
                    row[r++] = aiRow.get("ap_code" );     // [04] DEPART_AP_CODE
                    row[r++] = aiRow.get("ap_name");     // [05] DEPART_AP_NAME
                    row[r++] = aiRow.get( "ap_city" );     // [06] DEPART_AP_CITY
                    row[r++] = aiRow.get( "co_name" );     // [07] DEPART_AP_COUNTRY

                    // ARRIVAL AIRPORT
                    sb = new StringBuilder();
                    sb.append( "SELECT AP_CODE, AP_NAME, AP_CITY, AP_LONGITUDE, AP_LATITUDE, " );
                    sb.append( " CO_ID, CO_NAME, CO_CODE_2, CO_CODE_3, 1 FROM " );
                    sb.append( SEATSConstants.TABLENAME_AIRPORT );
                    sb.append( ", " );
                    sb.append( SEATSConstants.TABLENAME_COUNTRY );
                    sb.append( " WHERE AP_ID = " );
                    sb.append( f_arrive_airport );
                    sb.append( " AND AP_CO_ID = CO_ID");

                    aiRows = RestQuery.restReadQuery( sb.toString(), id );
                    aiRow = aiRows.get( 0 );

                    row[r++] = flightRow.get("f_arrive_time" );    // [08] ARRIVE_TIME
                    row[r++] = aiRow.get("ap_code");     // [09] ARRIVE_AP_CODE
                    row[r++] = aiRow.get("ap_name");     // [10] ARRIVE_AP_NAME
                    row[r++] = aiRow.get("ap_city");     // [11] ARRIVE_AP_CITY
                    row[r++] = aiRow.get("co_name");     // [12] ARRIVE_AP_COUNTRY
                    
                    finalResults.add(row);
                    if (debug)
                        LOG.debug(String.format("Flight %d / %s /  %s -> %s / %s",
                                                row[0], row[2], row[4], row[9], row[03]));

                } // Each Flight
            } // Each airport within range
        }
           
        if (debug) {
            LOG.debug("Flight Information:\n" + finalResults);
        }
        return (finalResults);
    }
}
