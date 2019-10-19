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
import java.util.Map;
import java.util.List;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * NewPurchase
 * Description goes here...
 * @author visawee
 */
public class NewPurchase extends Procedure {
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItemMaxBid = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ?"
    );
    
    public final SQLStmt getMaxBid = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
        " ORDER BY ib_bid DESC LIMIT 1" 
    );
    
    public final SQLStmt insertItemMaxBid = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " (" +
        "imb_i_id, " +
        "imb_u_id, " +
        "imb_ib_id, " +
        "imb_ib_i_id, " +
        "imb_ib_u_id, " +
        "imb_created, " +
        "imb_updated " +
        ") VALUES (" +
        "?, " + // imb_i_id
        "?, " + // imb_u_id
        "?, " + // imb_ib_id
        "?, " + // imb_ib_i_id
        "?, " + // imb_ib_u_id
        "?, " + // imb_created
        "? "  + // imb_updated
        ")"
    );
    
    public final SQLStmt getItemInfo = new SQLStmt(
        "SELECT i_num_bids, i_current_price, i_end_date, " +
        "       ib_id, ib_buyer_id, " +
        "       u_balance " +
		"  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_BID + ", " +
		            AuctionMarkConstants.TABLENAME_USERACCT +
        " WHERE i_id = ? AND i_u_id = ? " +
        "   AND imb_i_id = i_id AND imb_u_id = i_u_id " +
        "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id " +
        "   AND ib_buyer_id = u_id "
    );

    public final SQLStmt getBuyerInfo = new SQLStmt(
        "SELECT u_id, u_balance " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USERACCT +
        " WHERE u_id = ? "
    );
    
    public final SQLStmt insertPurchase = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " (" +
        	"ip_id," +
        	"ip_ib_id," +
        	"ip_ib_i_id," +  
        	"ip_ib_u_id," +  
        	"ip_date" +     
        ") VALUES(?,?,?,?,?)"
    );
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
          " SET i_status = " + ItemStatus.CLOSED.ordinal() + ", i_updated = ? " +
        " WHERE i_id = ? AND i_u_id = ? "
    );    
    
    public final SQLStmt updateUserItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + " " +
           "SET ui_ip_id = ?, " +
           "    ui_ip_ib_id = ?, " +
           "    ui_ip_ib_i_id = ?, " +
           "    ui_ip_ib_u_id = ?" +
        " WHERE ui_u_id = ? AND ui_i_id = ? AND ui_i_u_id = ?"
    );
    
    public final SQLStmt insertUserItem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + "(" +
            "ui_u_id, " +
            "ui_i_id, " +
            "ui_i_u_id, " +
            "ui_ip_id, " +
            "ui_ip_ib_id, " +
            "ui_ip_ib_i_id, " +
            "ui_ip_ib_u_id, " +
            "ui_created" +     
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    );
    
    public final SQLStmt updateUserBalance = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
           "SET u_balance = u_balance + ? " + 
        " WHERE u_id = ?"
    );
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public Object[] run(Connection conn, Timestamp benchmarkTimes[],
                        long item_id, long seller_id, long ip_id, double buyer_credit) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        
        PreparedStatement stmt = null;
        List<Map<String, Object>> results = null;
        long updated;
        boolean adv;
        StringBuilder sb;
        
        // HACK: Check whether we have an ITEM_MAX_BID record. If not, we'll insert one
        sb = new StringBuilder();
        sb.append("SELECT imb_i_id, imb_u_id FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
        sb.append(" WHERE imb_i_id = ");
        sb.append(item_id);
        sb.append(" AND imb_u_id = ");
        sb.append(seller_id);
        results = RestQuery.restReadQuery(sb.toString(), 0);

        if (results.isEmpty()) {
            // TODO: altered the where condition since there were no columns with those names
            // in the original table as near as I could tell
            sb = new StringBuilder();
            sb.append("SELECT ib_id, ib_i_id, ib_u_id, ib_bid FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
            sb.append(" WHERE ib_i_id = ");
            sb.append(item_id);
            sb.append(" AND ib_u_id = ");
            sb.append(seller_id);
            sb.append(" ORDER BY ib_bid DESC LIMIT 1");
            results = RestQuery.restReadQuery(sb.toString(), 0);
            assert(!results.isEmpty());
            long bid_id = (long)results.get(0).get("ib_id");

            sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
            sb.append("(imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated)");
            sb.append(" VALUES (");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(bid_id);
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(currentTime);
            sb.append(", ");
            sb.append(currentTime);
            sb.append(")");
            updated = RestQuery.restOtherQuery(sb.toString(), 0);
            assert(updated == 1) :
                String.format("Failed to update %s for Seller #%d's Item #%d",
                              AuctionMarkConstants.TABLENAME_ITEM_MAX_BID, seller_id, item_id);
        }
        
        // Get the ITEM_MAX_BID record so that we know what we need to process
        // At this point we should always have an ITEM_MAX_BID record
        sb = new StringBuilder();
        sb.append("SELECT i_num_bids, i_current_price, i_end_date, ib_id, ib_buyer_id, u_balance");
        sb.append(" FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(", ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
        sb.append(", ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
        sb.append(", ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(" WHERE i_id = ");
        sb.append(item_id);
        sb.append(" AND i_u_id = ");
        sb.append(seller_id);
        sb.append(" AND imb_i_id = i_id AND imb_u_id = i_u_id AND imb_ib_id = ib_id");
        sb.append(" AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id AND ib_buyer_id = u_id");
        results = RestQuery.restReadQuery(sb.toString(), 0);
        if (results.isEmpty()) {
            throw new UserAbortException("No ITEM_MAX_BID is available for item " + item_id);
        }

        long i_num_bids = (long)results.get(0).get("i_num_bids");
        double i_current_price = (double)results.get(0).get("i_current_price");
        Timestamp i_end_date = (Timestamp)results.get(0).get("i_end_date");
        ItemStatus i_status = ItemStatus.CLOSED;
        long ib_id = (long)results.get(0).get("ib_id");
        long ib_buyer_id = (long)results.get(0).get("ib_buyer_id");
        double u_balance = (double)results.get(0).get("u_balance");
        
        // Make sure that the buyer has enough money to cover this charge
        // We can add in a credit for the buyer's account
        if (i_current_price > (buyer_credit + u_balance)) {
            String msg = String.format("Buyer #%d does not have enough money in account to purchase Item #%d" +
                                       "[maxBid=%.2f, balance=%.2f, credit=%.2f]",
                                       ib_buyer_id, item_id, i_current_price, u_balance, buyer_credit);
            throw new UserAbortException(msg);
        }

        // Set item_purchase_id
        sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        sb.append(" (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id, ip_date)");
        sb.append(" VALUES (");
        sb.append(ip_id);
        sb.append(", ");
        sb.append(ib_id);
        sb.append(", ");
        sb.append(item_id);
        sb.append(", ");
        sb.append(seller_id);
        sb.append(", ");
        sb.append(currentTime);
        sb.append(")");
        updated = RestQuery.restOtherQuery(sb.toString(), 0);
        assert(updated == 1);
        
        // Update item status to close
        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(" SET i_status = ");
        sb.append(ItemStatus.CLOSED.ordinal());
        sb.append(", i_updated = ");
        sb.append(currentTime);
        sb.append(" WHERE i_id = ");
        sb.append(item_id);
        sb.append(" AND i_u_id = ");
        sb.append(seller_id);
        updated = RestQuery.restOtherQuery(sb.toString(), 0);
        assert(updated == 1) :
            String.format("Failed to update %s for Seller #%d's Item #%d",
                          AuctionMarkConstants.TABLENAME_ITEM, seller_id, item_id);
        
        // And update this the USERACT_ITEM record to link it to the new ITEM_PURCHASE record
        // If we don't have a record to update, just go ahead and create it
        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT_ITEM);
        sb.append(" SET ui_ip_id = ");
        sb.append(ip_id);
        sb.append(", ui_ip_ib_id = ");
        sb.append(ib_id);
        sb.append(", ui_ip_ib_i_id = ");
        sb.append(item_id);
        sb.append(", ui_ip_ib_u_id = ");
        sb.append(seller_id);
        sb.append(" WHERE ui_u_id = ");
        sb.append(ib_buyer_id);
        sb.append(" AND ui_i_id = ");
        sb.append(item_id);
        sb.append(" AND ui_i_u_id = ");
        sb.append(seller_id);
        updated = RestQuery.restOtherQuery(sb.toString(), 0);

        if (updated == 0) {
            sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(AuctionMarkConstants.TABLENAME_USERACCT_ITEM);
            sb.append(" (ui_u_id, ui_i_id, ui_i_u_id, ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id, ui_created)");
            sb.append(" VALUES (");
            sb.append(ib_buyer_id);
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(ip_id);
            sb.append(", ");
            sb.append(ib_id);
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(currentTime);
            sb.append(")");
            updated = RestQuery.restOtherQuery(sb.toString(), 0);
        }
        assert(updated == 1) :
            String.format("Failed to update %s for Buyer #%d's Item #%d",
                          AuctionMarkConstants.TABLENAME_USERACCT_ITEM, ib_buyer_id, item_id);
        
        // Decrement the buyer's account
        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(" SET u_balance = u_balance + ");
        sb.append(-1*(i_current_price) + buyer_credit);
        sb.append(" WHERE u_id = ");
        sb.append(ib_buyer_id);
        updated = RestQuery.restOtherQuery(sb.toString(), 0);
        assert(updated == 1) :
            String.format("Failed to update %s for Buyer #%d",
                          AuctionMarkConstants.TABLENAME_USERACCT, ib_buyer_id);
        
        // And credit the seller's account
        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(" SET u_balance = u_balance + ");
        sb.append(i_current_price);
        sb.append(" WHERE u_id = ");
        sb.append(seller_id);
        updated = RestQuery.restOtherQuery(sb.toString(), 0);
        assert(updated == 1) :
            String.format("Failed to update %s for Seller #%d",
                          AuctionMarkConstants.TABLENAME_USERACCT, seller_id);
        
        // Return a tuple of the item that we just updated
        return new Object[] {
            // ITEM ID
            item_id,
            // SELLER ID
            seller_id,
            // ITEM_NAME
            null,
            // CURRENT PRICE
            i_current_price,
            // NUM BIDS
            i_num_bids,
            // END DATE
            i_end_date,
            // STATUS
            i_status.ordinal(),
            // PURCHASE ID
            ip_id,
            // BID ID
            ib_id,
            // BUYER ID
            ib_buyer_id,
        };
    }	
}