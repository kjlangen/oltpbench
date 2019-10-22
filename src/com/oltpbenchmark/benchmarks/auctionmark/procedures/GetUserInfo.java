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
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * GetUserInfo
 * @author pavlo
 * @author visawee
 */
public class GetUserInfo extends Procedure {
    private static final Logger LOG = Logger.getLogger(GetUserInfo.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getUser = new SQLStmt(
        "SELECT u_id, u_rating, u_created, u_balance, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
          "FROM " + AuctionMarkConstants.TABLENAME_USERACCT + ", " +
                    AuctionMarkConstants.TABLENAME_REGION + " " +
         "WHERE u_id = ? AND u_r_id = r_id"
    );

    public final SQLStmt getUserFeedback = new SQLStmt(
        "SELECT u_id, u_rating, u_sattr0, u_sattr1, uf_rating, uf_date, uf_sattr0 " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USERACCT + ", " +
                    AuctionMarkConstants.TABLENAME_USERACCT_FEEDBACK +
        " WHERE u_id = ? AND uf_u_id = u_id " +
        " ORDER BY uf_date DESC LIMIT 25 "
    );

    public final SQLStmt getItemComments = new SQLStmt(
        "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR + ", " +
        "       ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_created " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " + 
                    AuctionMarkConstants.TABLENAME_ITEM_COMMENT +
        " WHERE i_u_id = ? AND i_status = ? "+ 
        "   AND i_id = ic_i_id AND i_u_id = ic_u_id AND ic_response IS NULL " +
        " ORDER BY ic_created DESC LIMIT 25 "
    );
    
    public final SQLStmt getSellerItems = new SQLStmt(
        "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR +
         " FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " +
         "WHERE i_u_id = ? " +
         "ORDER BY i_end_date DESC LIMIT 25 "
    );
    
    public final SQLStmt getBuyerItems = new SQLStmt(
        "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR +
         " FROM " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM +
        " WHERE ui_u_id = ? " +
           "AND ui_i_id = i_id AND ui_i_u_id = i_u_id " +
         "ORDER BY i_end_date DESC LIMIT 25 "
    );
    
    public final SQLStmt getWatchedItems = new SQLStmt(
        "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR + ", uw_u_id, uw_created " +
          "FROM " + AuctionMarkConstants.TABLENAME_USERACCT_WATCH + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM +
        " WHERE uw_u_id = ? " +
        "   AND uw_i_id = i_id AND uw_i_u_id = i_u_id " +
        " ORDER BY i_end_date DESC LIMIT 25"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    /**
     * 
     * @param conn
     * @param benchmarkTimes
     * @param user_id
     * @param get_feedback
     * @param get_comments
     * @param get_seller_items
     * @param get_buyer_items
     * @param get_watched_items
     * @return
     * @throws SQLException
     */
    public List<Object[]>[] run(Connection conn, Timestamp benchmarkTimes[],
                                long user_id,
                                boolean get_feedback,
                                boolean get_comments,
                                boolean get_seller_items,
                                boolean get_buyer_items,
                                boolean get_watched_items,
				int clientId) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        
        Map<Integer, List<Map<String, Object>>> results = new HashMap<>();
        Map<Integer, String> select_lists = new HashMap<>();
        int result_idx = 0;
        
        // The first VoltTable in the output will always be the user's information
        if (debug) LOG.debug("Grabbing USER record: " + user_id);
        select_lists.put(result_idx, "u_id, u_rating, u_created, u_balance, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name");
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(select_lists.get(result_idx));
        sb.append(" FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(", ");
        sb.append(AuctionMarkConstants.TABLENAME_REGION);
        sb.append(" WHERE u_id = ");
        sb.append(user_id);
        sb.append(" AND u_r_id = r_id");
        results.put(result_idx, RestQuery.restReadQuery(sb.toString(), 0));
        result_idx++;

        // They can also get their USER_FEEDBACK records if they want as well
        if (get_feedback) {
            if (debug) LOG.debug("Grabbing USER_FEEDBACK records: " + user_id);

            select_lists.put(result_idx, "u_id, u_rating, u_sattr0, u_sattr1, uf_rating, uf_date, uf_sattr0");
            sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(select_lists.get(result_idx));
            sb.append(" FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
            sb.append(", ");
            sb.append(AuctionMarkConstants.TABLENAME_USERACCT_FEEDBACK);
            sb.append(" WHERE u_id = ");
            sb.append(user_id);
            sb.append(" AND uf_u_id = u_id ORDER BY uf_date DESC LIMIT 25");
            results.put(result_idx, RestQuery.restReadQuery(sb.toString(), 0));
        }
        result_idx++;
        
        // And any pending ITEM_COMMENTS that need a response
        if (get_comments) {
            if (debug) LOG.debug("Grabbing ITEM_COMMENT records: " + user_id);
            
            select_lists.put(result_idx, AuctionMarkConstants.ITEM_COLUMNS_STR + ", ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_created");
            sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(select_lists.get(result_idx));
            sb.append(" FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(", ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_COMMENT);
            sb.append(" WHERE i_u_id = ");
            sb.append(user_id);
            sb.append(" AND i_status = ");
            sb.append(ItemStatus.OPEN.ordinal());
            sb.append(" AND i_id = ic_i_id AND i_u_id = ic_u_id AND ic_response IS NULL");
            sb.append(" ORDER BY ic_created DESC LIMIT 25");
            results.put(result_idx, RestQuery.restReadQuery(sb.toString(), 0));
        }
        result_idx++;
        
        // The seller's items
        if (get_seller_items) {
            if (debug) LOG.debug("Grabbing seller's ITEM records: " + user_id);
            
            select_lists.put(result_idx, AuctionMarkConstants.ITEM_COLUMNS_STR);
            sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(select_lists.get(result_idx));
            sb.append(" FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" WHERE i_u_id = ");
            sb.append(user_id);
            sb.append(" ORDER BY i_end_date DESC LIMIT 25");
            results.put(result_idx, RestQuery.restReadQuery(sb.toString(), 0));
        }
        result_idx++;

        // The buyer's purchased items
        if (get_buyer_items) {
            // 2010-11-15: The distributed query planner chokes on this one and makes a plan
            // that basically sends the entire user table to all nodes. So for now we'll just execute
            // the query to grab the buyer's feedback information
            if (debug) LOG.debug("Grabbing buyer's USER_ITEM records: " + user_id);
            
            select_lists.put(result_idx, AuctionMarkConstants.ITEM_COLUMNS_STR);
            sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(select_lists.get(result_idx));
            sb.append(" FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_USERACCT_ITEM);
            sb.append(", ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" WHERE ui_u_id = ");
            sb.append(user_id);
            sb.append(" AND ui_i_id = i_id AND ui_i_u_id = i_u_id ORDER BY i_end_date DESC LIMIT 25");
            results.put(result_idx, RestQuery.restReadQuery(sb.toString(), 0));
        }
        result_idx++;
        
        // The buyer's watched items
        if (get_watched_items) {
            if (debug) LOG.debug("Grabbing buyer's USER_WATCH records: " + user_id);
            
            select_lists.put(result_idx, AuctionMarkConstants.ITEM_COLUMNS_STR + ", uw_u_id, uw_created");
            sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(select_lists.get(result_idx));
            sb.append(" FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_USERACCT_WATCH);
            sb.append(", ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" WHERE uw_u_id = ");
            sb.append(user_id);
            sb.append(" AND uw_i_id = i_id AND uw_i_u_id = i_u_id ORDER BY i_end_date DESC LIMIT 25");
            results.put(result_idx, RestQuery.restReadQuery(sb.toString(), 0));
        }
        result_idx++;

        @SuppressWarnings("unchecked")
        List<Object[]> final_results[] = new List[(int) result_idx];
        for(int i = 0; i < result_idx; i++) {
            // The rows of the ith result
            List<Object[]> inner = null;

            // Only add rows if we did the ith query
            if (results.get(i) != null) {
                // Concretely instantiate the rows of the ith result
                inner = new ArrayList<Object[]>();
                // Count the number of columns in the ith query
                String cols[] = select_lists.get(i).split(", ");

                // Rows
                for (Map<String, Object> row : results.get(i)) {
                    // The row as an object array representation
                    Object obj_row[] = new Object[cols.length];

                    // Columns
                    for (int j = 0; j < cols.length; j++) {
                        obj_row[j] = row.get(cols[j]);
                    }
                    inner.add(obj_row);
                }
            }

            // Add the rows of the ith result into the array of final results
            final_results[i] = inner;
        }

        return (final_results);
    }
}
