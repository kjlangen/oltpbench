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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;


import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.benchmarks.seats.util.ErrorType;
import com.oltpbenchmark.benchmarks.seats.util.RestQuery;

public class UpdateCustomer extends Procedure {
    private static final Logger LOG = Logger.getLogger(UpdateCustomer.class);
    
    public final SQLStmt GetCustomerIdStr = new SQLStmt(
        "SELECT C_ID " +
        "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID_STR = ? "
    );
    
    public final SQLStmt GetCustomer = new SQLStmt(
        "SELECT * " +
        "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID = ? "
    );
    
    public final SQLStmt GetBaseAirport = new SQLStmt(
        "SELECT * " +
        "  FROM " + SEATSConstants.TABLENAME_AIRPORT + ", " +
                    SEATSConstants.TABLENAME_COUNTRY +
        " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
    );
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
        "UPDATE " + SEATSConstants.TABLENAME_CUSTOMER +
        "   SET C_IATTR00 = ?, " +
        "       C_IATTR01 = ? " +
        " WHERE C_ID = ?"
    );
    
    public final SQLStmt GetFrequentFlyers = new SQLStmt(
        "SELECT * FROM " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
        " WHERE FF_C_ID = ?"
    );
            
    public final SQLStmt UpdatFrequentFlyers = new SQLStmt(
        "UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
        "   SET FF_IATTR00 = ?, " +
        "       FF_IATTR01 = ? " +
        " WHERE FF_C_ID = ? " + 
        "   AND FF_AL_ID = ? "
    );
    
    public void run(Connection conn, Long c_id, String c_id_str, Long update_ff, long attr0, long attr1, int id ) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        
        // Use C_ID_STR to get C_ID
        if (c_id == null) {

            StringBuilder sb = new StringBuilder();
            assert(c_id_str != null);
            assert(c_id_str.isEmpty() == false);

            sb.append( "SELECT C_ID FROM " );
            sb.append( SEATSConstants.TABLENAME_CUSTOMER );
            sb.append( " WHERE C_ID_STR = '" );
            sb.append( c_id_str );
            sb.append( "'" );

            List<Map<String,Object>> resultSet = RestQuery.restReadQuery( sb.toString(), id );
            if( !resultSet.isEmpty() ) {
                Map<String,Object> row = resultSet.get( 0 );
                c_id = (Long) row.get( "c_id" );
            } else {
                throw new UserAbortException(String.format("No Customer information record found for string '%s'", c_id_str));
            }
        }

        assert(c_id != null);
        
        // Normally the retrieve all fields here but they are unused...
        StringBuilder sb = new StringBuilder();
        sb.append( "SELECT C_ID, C_ID_STR, C_BASE_AP_ID, C_BALANCE FROM " );
        sb.append( SEATSConstants.TABLENAME_CUSTOMER );
        sb.append( " WHERE C_ID = " );
        sb.append( c_id );
        List<Map<String,Object>> resultSet = RestQuery.restReadQuery( sb.toString(), id );
        if( resultSet.isEmpty() ) {
            throw new UserAbortException(String.format("No Customer information record found for id '%d'", c_id));
        }
        Map<String, Object> apRow = resultSet.get( 0 );
        long base_airport = new Long( (Integer) apRow.get( "c_base_ap_id" ) );
        
        // Get their airport information
        sb = new StringBuilder();
        sb.append( "SELECT AP_ID FROM " );
        sb.append( SEATSConstants.TABLENAME_AIRPORT );
        sb.append( "," );
        sb.append( SEATSConstants.TABLENAME_COUNTRY );
        sb.append( " WHERE AP_ID = " );
        sb.append( base_airport );
        sb.append( " AND AP_CO_ID = CO_ID" );
        
        resultSet = RestQuery.restReadQuery( sb.toString(), id );
        assert( !resultSet.isEmpty() );
        
        if (update_ff != null) {
            sb = new StringBuilder();
            sb.append( "SELECT FF_AL_ID FROM " );
            sb.append( SEATSConstants.TABLENAME_FREQUENT_FLYER );
            sb.append( " WHERE FF_C_ID = " );
            sb.append( c_id );
            resultSet = RestQuery.restReadQuery( sb.toString(), id );
            for( Map<String,Object> ffRow : resultSet ) {
                long ff_al_id = new Long( (Integer) ffRow.get( "ff_al_id" ) );
                sb = new StringBuilder();
                sb.append( "UPDATE " );
                sb.append( SEATSConstants.TABLENAME_FREQUENT_FLYER );
                sb.append( " SET FF_IATTR00 = ");
                sb.append( attr0 );
                sb.append( ", FF_IATTR01 = " );
                sb.append( attr1 );
                sb.append( " WHERE FF_C_ID = " );
                sb.append( c_id );
                sb.append( " AND FF_AL_ID = " );
                sb.append( ff_al_id );
                RestQuery.restOtherQuery( sb.toString(), id );
            }
        }

        sb = new StringBuilder();
        sb.append( "UPDATE " );
        sb.append( SEATSConstants.TABLENAME_CUSTOMER );
        sb.append( " SET C_IATTR00 = " );
        sb.append( attr0 );
        sb.append( " , C_IATTR01 = " );
        sb.append( attr1 );
        sb.append( " WHERE C_ID = " );
        sb.append( c_id );
        int updated = RestQuery.restOtherQuery( sb.toString(), id );
        if (updated != 1) {
            String msg = String.format("Failed to update customer #%d - Updated %d records", c_id, updated);
            if (debug) LOG.warn(msg);
            throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
        }
        
        return;
    }
}
