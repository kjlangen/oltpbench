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
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * NewBid
 * @author pavlo
 * @author visawee
 */
public class NewBid extends Procedure {
    private static final Logger LOG = Logger.getLogger(NewBid.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getItem = new SQLStmt(
        "SELECT i_initial_price, i_current_price, i_num_bids, i_end_date, i_status " +
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE i_id = ? AND i_u_id = ? "
    );
    
    public final SQLStmt getMaxBidId = new SQLStmt(
        "SELECT MAX(ib_id) " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE ib_i_id = ? AND ib_u_id = ? "
    );
    
    public final SQLStmt getItemMaxBid = new SQLStmt(
        "SELECT imb_ib_id, ib_bid, ib_max_bid, ib_buyer_id " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
        "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id "
    );
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_num_bids = i_num_bids + 1, " +
        "       i_current_price = ?, " +
        "       i_updated = ? " +
        " WHERE i_id = ? AND i_u_id = ? "
    );
    
    public final SQLStmt updateItemMaxBid = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + 
        "   SET imb_ib_id = ?, " +
        "       imb_ib_i_id = ?, " +
        "       imb_ib_u_id = ?, " +
        "       imb_updated = ? " +
        " WHERE imb_i_id = ? " +
        "   AND imb_u_id = ?"
    );
    
    public final SQLStmt updateBid = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_BID + 
        "   SET ib_bid = ?, " +
        "       ib_max_bid = ?, " +
        "       ib_updated = ? " +
        " WHERE ib_id = ? " +
        "   AND ib_i_id = ? " +
        "   AND ib_u_id = ? "
    );
    
    public final SQLStmt insertItemBid = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_BID + " (" +
        "ib_id, " +
        "ib_i_id, " +
        "ib_u_id, " + 
        "ib_buyer_id, " +
        "ib_bid, " +
        "ib_max_bid, " +
        "ib_created, " +
        "ib_updated " +
        ") VALUES (" +
        "?, " + // ib_id
        "?, " + // ib_i_id
        "?, " + // ib_u_id
        "?, " + // ib_buyer_id
        "?, " + // ib_bid
        "?, " + // ib_max_bid
        "?, " + // ib_created
        "? "  + // ib_updated
        ")"
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

    public Object[] run(Connection conn, Timestamp benchmarkTimes[],
                        long item_id, long seller_id, long buyer_id, double newBid, Timestamp estimatedEndDate) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();
        if (debug) LOG.debug(String.format("Attempting to place new bid on Item %d [buyer=%d, bid=%.2f]",
                                           item_id, buyer_id, newBid));

        // PreparedStatement stmt = null;
        // ResultSet results = null;
        StringBuilder sb = null;
        List<Map<String, Object>> results = null;
        
        // Check to make sure that we can even add a new bid to this item
        // If we fail to get back an item, then we know that the auction is closed
        sb = new StringBuilder();
        sb.append("SELECT i_initial_price, i_current_price, i_num_bids, i_end_date, i_status");
        sb.append(" FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(" WHERE i_id = ");
        sb.append(item_id);
        sb.append(" AND i_u_id = ");
        sb.append(seller_id);
        results = RestQuery.restReadQuery(sb.toString(), 0);
        if (results.isEmpty()) {
            throw new UserAbortException("Invalid item " + item_id);
        }
        double i_initial_price = (double)results.get(0).get("i_initial_price");
        double i_current_price = (double)results.get(0).get("i_current_price");
        long i_num_bids = (long)results.get(0).get("i_num_bids");
        Timestamp i_end_date = (Timestamp)results.get(0).get("i_end_date");
        ItemStatus i_status = ItemStatus.get((long)results.get(0).get("i_status"));
        long newBidId = 0;
        long newBidMaxBuyerId = buyer_id;
        
//        if (i_end_date.compareTo(currentTime) < 0 || i_status != ItemStatus.OPEN) {
//            if (debug)
//                LOG.debug(String.format("The auction for item %d has ended [status=%s]\nCurrentTime:\t%s\nActualEndDate:\t%s\nEstimatedEndDate:\t%s",
//                                        item_id, i_status, currentTime, i_end_date, estimatedEndDate));
//            throw new UserAbortException("Unable to bid on item: Auction has ended");
//        }
        
        // If we existing bids, then we need to figure out whether we are the new highest
        // bidder or if the existing one just has their max_bid bumped up
        if (i_num_bids > 0) {
            // Get the next ITEM_BID id for this item
            if (debug) LOG.debug("Retrieving ITEM_MAX_BID information for " + ItemId.toString(item_id));

            sb = new StringBuilder();
            sb.append("SELECT MAX(ib_id) FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
            sb.append(" WHERE ib_i_id = ");
            sb.append(item_id);
            sb.append(" AND ib_u_id = ");
            sb.append(seller_id);
            results = RestQuery.restReadQuery(sb.toString(), 0);
            assert(!results.isEmpty());
            newBidId = (long)results.get(0).get("ib_id") + 1;
            
            // Get the current max bid record for this item
            sb = new StringBuilder();
            sb.append("SELECT imb_ib_id, ib_bid, ib_max_bid, ib_buyer_id FROM");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
            sb.append(", ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
            sb.append(" WHERE imb_i_id = ");
            sb.append(item_id);
            sb.append(" AND imb_u_id = ");
            sb.append(seller_id);
            sb.append(" AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id ");
            results = RestQuery.restReadQuery(sb.toString(), 0);
            assert(!results.isEmpty());

            long currentBidId = (long)results.get(0).get("imb_ib_id");
            double currentBidAmount = (double)results.get(0).get("ib_bid");
            double currentBidMax = (double)results.get(0).get("ib_max_bid");
            long currentBuyerId = (long)results.get(0).get("ib_buyer_id");
            
            boolean updateMaxBid = false;
            assert((int)currentBidAmount == (int)i_current_price) :
                String.format("%.2f == %.2f", currentBidAmount, i_current_price);
            
            // Check whether this bidder is already the max bidder
            // This means we just need to increase their current max bid amount without
            // changing the current auction price
            if (buyer_id == currentBuyerId) {
                if (newBid < currentBidMax) {
                    String msg = String.format("%s is already the highest bidder for Item %d but is trying to " +
                                               "set a new max bid %.2f that is less than current max bid %.2f",
                                               buyer_id, item_id, newBid, currentBidMax);
                    if (debug) LOG.debug(msg);
                    throw new UserAbortException(msg);
                }
                sb = new StringBuilder();
                sb.append("UPDATE ");
                sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
                sb.append(" SET ib_bid = ");
                sb.append(i_current_price);
                sb.append(", ib_max_bid = ");
                sb.append(newBid);
                sb.append(", ib_updated = ");
                sb.append(currentTime);
                sb.append(" WHERE ib_id = ");
                sb.append(currentBidId);
                sb.append(" AND ib_i_id = ");
                sb.append(item_id);
                sb.append(" AND ib_u_id = ");
                sb.append(seller_id);
                RestQuery.restOtherQuery(sb.toString(), 0);

                if (debug) LOG.debug(String.format("Increasing the max bid the highest bidder %s from %.2f to %.2f for Item %d",
                                                   buyer_id, currentBidMax, newBid, item_id));
            }
            // Otherwise check whether this new bidder's max bid is greater than the current max
            else {
                // The new maxBid trumps the existing guy, so our the buyer_id for this txn becomes the new
                // winning bidder at this time. The new current price is one step above the previous
                // max bid amount 
                if (newBid > currentBidMax) {
                    i_current_price = Math.min(newBid, currentBidMax + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));
                    assert(i_current_price > currentBidMax);
                    // Defer the update to ITEM_MAX_BID until after we insert our new ITEM_BID record
                    updateMaxBid = true;
                }
                // The current max bidder is still the current one
                // We just need to bump up their bid amount to be at least the bidder's amount
                // Make sure that we don't go over the the currentMaxBidMax, otherwise this would mean
                // that we caused the user to bid more than they wanted.
                else {
                    newBidMaxBuyerId = currentBuyerId;
                    i_current_price = Math.min(currentBidMax, newBid + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));
                    assert(i_current_price >= newBid) : String.format("%.2f > %.2f", i_current_price, newBid);

                    sb = new StringBuilder();
                    sb.append("UPDATE ");
                    sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
                    sb.append(" SET ib_bid = ");
                    sb.append(i_current_price);
                    sb.append(", ib_max_bid = ");
                    sb.append(i_current_price);
                    sb.append(", ib_updated = ");
                    sb.append(currentTime);
                    sb.append(" WHERE ib_id = ");
                    sb.append(currentBidId);
                    sb.append(" AND ib_i_id = ");
                    sb.append(item_id);
                    sb.append(" AND ib_u_id = ");
                    sb.append(seller_id);
                    RestQuery.restOtherQuery(sb.toString(), 0);

                    if (debug) LOG.debug(String.format("Keeping the existing highest bidder of Item %d as %s but updating current price from %.2f to %.2f",
                                                       item_id, buyer_id, currentBidAmount, i_current_price));
                }
            
                // Always insert an new ITEM_BID record even if BuyerId doesn't become
                // the new highest bidder. We also want to insert a new record even if
                // the BuyerId already has ITEM_BID record, because we want to maintain
                // the history of all the bid attempts
                sb = new StringBuilder();
                sb.append("INSERT INTO ");
                sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
                sb.append(" (ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated) VALUES (");
                sb.append(newBidId);
                sb.append(", ");
                sb.append(item_id);
                sb.append(", ");
                sb.append(seller_id);
                sb.append(", ");
                sb.append(buyer_id);
                sb.append(", ");
                sb.append(i_current_price);
                sb.append(", ");
                sb.append(newBid);
                sb.append(", ");
                sb.append(currentTime);
                sb.append(", ");
                sb.append(currentTime);
                sb.append(")");
                RestQuery.restOtherQuery(sb.toString(), 0);

                sb = new StringBuilder();
                sb.append("UPDATE ");
                sb.append(AuctionMarkConstants.TABLENAME_ITEM);
                sb.append(" SET i_num_bids = i_num_bids + 1, i_current_price = ");
                sb.append(i_current_price);
                sb.append(", i_updated = ");
                sb.append(currentTime);
                sb.append(" WHERE i_id = ");
                sb.append(item_id);
                sb.append(" AND i_u_id = ");
                sb.append(seller_id);
                RestQuery.restOtherQuery(sb.toString(), 0);
                
                // This has to be done after we insert the ITEM_BID record to make sure
                // that the HSQLDB test cases work
                if (updateMaxBid) {
                    sb = new StringBuilder();
                    sb.append("UPDATE ");
                    sb.append(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
                    sb.append(" SET imb_ib_id = ");
                    sb.append(newBidId);
                    sb.append(", imb_ib_i_id = ");
                    sb.append(item_id);
                    sb.append(", imb_ib_u_id = ");
                    sb.append(seller_id);
                    sb.append(", imb_updated = ");
                    sb.append(currentTime);
                    sb.append(" WHERE imb_i_id = ");
                    sb.append(item_id);
                    sb.append(" AND imb_u_id = ");
                    sb.append(seller_id);
                    RestQuery.restOtherQuery(sb.toString(), 0);

                    if (debug) LOG.debug(String.format("Changing new highest bidder of Item %d to %s [newMaxBid=%.2f > currentMaxBid=%.2f]",
                                         item_id, UserId.toString(buyer_id), newBid, currentBidMax));
                }
            }
        }
        // There is no existing max bid record, therefore we can just insert ourselves
        else {
            sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
            sb.append(" (ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated) VALUES (");
            sb.append(newBidId);
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(buyer_id);
            sb.append(", ");
            sb.append(i_initial_price);
            sb.append(", ");
            sb.append(newBid);
            sb.append(", ");
            sb.append(currentTime);
            sb.append(", ");
            sb.append(currentTime);
            sb.append(")");
            RestQuery.restOtherQuery(sb.toString(), 0);
            
            sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
            sb.append(" (imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated) VALUES (");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(newBidId);
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(currentTime);
            sb.append(", ");
            sb.append(currentTime);
            sb.append(")");
            RestQuery.restOtherQuery(sb.toString(), 0);

            sb = new StringBuilder();
            sb.append("UPDATE ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" SET i_num_bids = i_num_bids + 1, i_current_price = ");
            sb.append(i_current_price);
            sb.append(", i_updated = ");
            sb.append(currentTime);
            sb.append(" WHERE i_id = ");
            sb.append(item_id);
            sb.append(" AND i_u_id = ");
            sb.append(seller_id);
            RestQuery.restOtherQuery(sb.toString(), 0);

            if (debug) LOG.debug(String.format("Creating the first bid record for Item %d and setting %s as highest bidder at %.2f",
                                               item_id, buyer_id, i_current_price));
        }
        
        // Return back information about the current state of the item auction
        return new Object[] {
            // ITEM_ID
            item_id,
            // SELLER_ID
            seller_id,
            // ITEM_NAME
            null, // ignore
            // CURRENT PRICE
            i_current_price,
            // NUM BIDS
            i_num_bids + 1,
            // END DATE
            i_end_date,
            // STATUS
            i_status.ordinal(),
            // MAX BID ID
            newBidId,
            // MAX BID BUYER_ID
            newBidMaxBuyerId,
        };
    }
}
