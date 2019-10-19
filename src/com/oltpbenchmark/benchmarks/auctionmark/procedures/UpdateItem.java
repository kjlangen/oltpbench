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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * UpdateItem
 * @author pavlo
 * @author visawee
 */
public class UpdateItem extends Procedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_description = ?, " +
        "       i_updated = ? " +
        " WHERE i_id = ? AND i_u_id = ? "
        // "   AND i_status = " + ItemStatus.OPEN.ordinal()
    );
    
    public final SQLStmt deleteItemAttribute = new SQLStmt(
        "DELETE FROM " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE +
        " WHERE ia_id = ? AND ia_i_id = ? AND ia_u_id = ?"
    );

    public final SQLStmt getMaxItemAttributeId = new SQLStmt(
        "SELECT MAX(ia_id) FROM " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE +
        " WHERE ia_i_id = ? AND ia_u_id = ?"
    );
    
    public final SQLStmt insertItemAttribute = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE + " (" +
            "ia_id," + 
            "ia_i_id," + 
            "ia_u_id," + 
            "ia_gav_id," + 
            "ia_gag_id" + 
        ") VALUES (?, ?, ?, ?, ?)"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

	/**
	 * The buyer modifies an existing auction that is still available.
	 * The transaction will just update the description of the auction.
	 * A small percentage of the transactions will be for auctions that are
	 * uneditable (1.0%?); when this occurs, the transaction will abort.
	 */
    public boolean run(Connection conn, Timestamp benchmarkTimes[],
                       long item_id, long seller_id, String description,
                       boolean delete_attribute, long add_attribute[]) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(" SET i_description = ");
        sb.append(RestQuery.quoteAndSanitize(description));
        sb.append(", i_updated = ");
        sb.append(currentTime);
        sb.append(" WHERE i_id = ");
        sb.append(item_id);
        sb.append(" AND i_u_id = ");
        sb.append(seller_id);
        long updated = RestQuery.restOtherQuery(sb.toString(), 0);
        if (updated == 0) {
            throw new UserAbortException("Unable to update closed auction");
        }
        
        // DELETE ITEM_ATTRIBUTE
        if (delete_attribute) {
            // Only delete the first (if it even exists)
            long ia_id = AuctionMarkUtil.getUniqueElementId(item_id, 0);
            sb = new StringBuilder();
            sb.append("DELETE FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE);
            sb.append(" WHERE ia_id = ");
            sb.append(ia_id);
            sb.append(" AND ia_i_id = ");
            sb.append(item_id);
            sb.append(" AND ia_u_id = ");
            sb.append(seller_id);
            updated = RestQuery.restOtherQuery(sb.toString(), 0);

        }
        // ADD ITEM_ATTRIBUTE
        if (add_attribute.length > 0 && add_attribute[0] != -1) {
            assert(add_attribute.length == 2);
            long gag_id = add_attribute[0];
            long gav_id = add_attribute[1];
            long ia_id = -1;
            
            sb = new StringBuilder();
            sb.append("SELECT MAX(ia_id) FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE);
            sb.append(" WHERE ia_i_id = ");
            sb.append(item_id);
            sb.append(" AND ia_u_id = ");
            sb.append(seller_id);
            List<Map<String, Object>> results = RestQuery.restReadQuery(sb.toString(), 0);
            if (!results.isEmpty()) {
                ia_id = (long)results.get(0).get("ia_id");
            } else {
                ia_id = AuctionMarkUtil.getUniqueElementId(item_id, 0);
            }
            assert(ia_id > 0);

            sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE);
            sb.append(" (ia_id, ia_i_id, ia_u_id, ia_gav_id, ia_gag_id)");
            sb.append(" VALUES (");
            sb.append(ia_id);
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(gag_id);
            sb.append(", ");
            sb.append(gav_id);
            sb.append(")");
            updated = RestQuery.restOtherQuery(sb.toString(), 0);
            assert(updated == 1);
        }
        
        return (true);
    }
}