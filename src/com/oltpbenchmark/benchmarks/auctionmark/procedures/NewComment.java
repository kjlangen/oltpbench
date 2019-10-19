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
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * NewComment
 * @author visawee
 */
public class NewComment extends Procedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItemComments = new SQLStmt(
        "SELECT i_num_comments " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + 
        " WHERE i_id = ? AND i_u_id = ?"
    );
    
    public final SQLStmt updateItemComments = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_num_comments = i_num_comments + 1 " + 
        " WHERE i_id = ? AND i_u_id = ?"
    );
	
    public final SQLStmt insertItemComment = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + "(" +
        	"ic_id," +
        	"ic_i_id," +
        	"ic_u_id," +
        	"ic_buyer_id," +
        	"ic_question, " +
        	"ic_created," +
        	"ic_updated " +
        ") VALUES (?,?,?,?,?,?,?)"
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
           "SET u_comments = u_comments + 1, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );
	
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public Object[] run(Connection conn, Timestamp benchmarkTimes[],
                        long item_id, long seller_id, long buyer_id, String question) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
    	
        // Set comment_id
        long ic_id = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT i_num_comments");
        sb.append(" FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(" WHERE i_id = ");
        sb.append(item_id);
        sb.append(" AND i_u_id = ");
        sb.append(seller_id);
        List<Map<String, Object>> results = RestQuery.restReadQuery(sb.toString(), 0);
        if (!results.isEmpty()) {
            ic_id = (long)results.get(0).get("i_num_comments") + 1;
        }

        sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM_COMMENT);
        sb.append("(ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_created, ic_updated)");
        sb.append(" VALUES (");
        sb.append(ic_id);
        sb.append(", ");
        sb.append(item_id);
        sb.append(", ");
        sb.append(seller_id);
        sb.append(", ");
        sb.append(buyer_id);
        sb.append(", ");
        sb.append(RestQuery.quoteAndSanitize(question));
        sb.append(", ");
        sb.append(currentTime);
        sb.append(", ");
        sb.append(currentTime);
        sb.append(")");
        RestQuery.restOtherQuery(sb.toString(), 0);

        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(" SET i_num_comments = i_num_comments + 1");
        sb.append(" WHERE i_id = ");
        sb.append(item_id);
        sb.append(" AND i_u_id = ");
        sb.append(seller_id);
        RestQuery.restOtherQuery(sb.toString(), 0);

        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(" SET u_comments = u_comments + 1, u_updated = ");
        sb.append(currentTime);
        sb.append(" WHERE u_id = ");
        sb.append(seller_id);
        RestQuery.restOtherQuery(sb.toString(), 0);

        // Return new ic_id
        return (new Object[]{ ic_id, item_id, seller_id });
    }	
	
}