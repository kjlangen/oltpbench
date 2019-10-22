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
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * NewCommentResponse
 * @author pavlo
 * @author visawee
 */
public class NewCommentResponse extends Procedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateComment = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + " " +
        	"SET ic_response = ?, " +
        	"    ic_updated = ? " +
        "WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ? "
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
           "SET u_comments = u_comments - 1, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public void run(Connection conn, Timestamp benchmarkTimes[],
                    long item_id, long seller_id, long comment_id, String response, int clientId) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);

        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM_COMMENT);
        sb.append(" SET ic_response = ");
        sb.append(RestQuery.quoteAndSanitize(response));
        sb.append(", ic_updated = '");
        sb.append(currentTime);
        sb.append("' WHERE ic_id = ");
        sb.append(comment_id);
        sb.append(" AND ic_i_id = ");
        sb.append(item_id);
        sb.append(" AND ic_u_id = ");
        sb.append(seller_id);
        RestQuery.restOtherQuery(sb.toString(), clientId);

        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(" SET u_comments = u_comments - 1, u_updated = '");
        sb.append(currentTime);
        sb.append("' WHERE u_id = ");
        sb.append(seller_id);
        RestQuery.restOtherQuery(sb.toString(), clientId);

        return;
    }	
}
