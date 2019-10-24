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


package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * Get Item Information
 * Returns all of the attributes for a particular item
 * @author pavlo
 */
public class GetItem extends Procedure {

    private static final Logger LOG = Logger.getLogger( GetItem.class );

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItem = new SQLStmt(
        "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR +
         " FROM " + AuctionMarkConstants.TABLENAME_ITEM + 
         " WHERE i_id = ? AND i_u_id = ?" 
    );
    
    public final SQLStmt getUser = new SQLStmt(
        "SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USERACCT + ", " +
                    AuctionMarkConstants.TABLENAME_REGION +
        " WHERE u_id = ? AND u_r_id = r_id"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public Object[][] run(Connection conn, Timestamp benchmarkTimes[],
                          long item_id, long seller_id, int clientId) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT");
	sb.append( " i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status" );
        sb.append(" FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(" WHERE i_id = ");
        sb.append(item_id);
        sb.append(" AND i_u_id = ");
        sb.append(seller_id);
        List<Map<String, Object>> item_results = RestQuery.restReadQuery(sb.toString(), clientId );
        if (item_results.size() == 0) {
            throw new UserAbortException("Invalid item " + item_id);
        }
        Object item_row[] = new Object[AuctionMarkConstants.ITEM_COLUMNS.length];

	item_row[0] = (Object) new Long( item_id );
        for (int i = 1; i < item_row.length; i++) {
            item_row[i] = item_results.get(0).get(AuctionMarkConstants.ITEM_COLUMNS[i]);
        }
        
        String user_columns = "u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name";
        sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(user_columns);
        sb.append(" FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(", ");
        sb.append(AuctionMarkConstants.TABLENAME_REGION);
        sb.append(" WHERE u_id = ");
        sb.append(seller_id);
        sb.append(" AND u_r_id = r_id");
        List<Map<String, Object>> user_results = RestQuery.restReadQuery(sb.toString(), clientId );

        Object user_row[] = null;
        if (user_results.size() == 0) {
            throw new UserAbortException("Invalid user id " + seller_id);
        }
        String[] user_columns_arr = user_columns.split(", ");
        user_row = new Object[user_columns_arr.length+1];
	user_row[0] = seller_id;
        for (int i = 0; i < user_columns_arr.length; i++) {
            user_row[i+1] = user_results.get(0).get(user_columns_arr[i]);
        }
        
        return (new Object[][]{ item_row, user_row });
    }
    
}
