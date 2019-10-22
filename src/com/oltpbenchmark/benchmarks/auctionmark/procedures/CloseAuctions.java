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
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * PostAuction
 * @author pavlo
 * @author visawee
 */
public class CloseAuctions extends Procedure {
    private static final Logger LOG = Logger.getLogger(CloseAuctions.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getDueItems = new SQLStmt(
        "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR + 
         " FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE (i_start_date BETWEEN ? AND ?) " +
         "AND ? " +
         "ORDER BY i_id ASC " +
         "LIMIT " + AuctionMarkConstants.CLOSE_AUCTIONS_ITEMS_PER_ROUND
    );
    
    public final SQLStmt getMaxBid = new SQLStmt(
        "SELECT imb_ib_id, ib_buyer_id " + 
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
           "AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id "
    );
    
    public final SQLStmt updateItemStatus = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
           "SET i_status = ?, " +
           "    i_updated = ? " +
        "WHERE i_id = ? AND i_u_id = ? "
    );

    public final SQLStmt insertUserItem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + "(" +
            "ui_u_id, " +
            "ui_i_id, " +
            "ui_i_u_id, " +  
            "ui_created" +     
        ") VALUES(?, ?, ?, ?)"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    /**
     * @param item_ids - Item Ids
     * @param seller_ids - Seller Ids
     * @param bid_ids - ItemBid Ids
     * @return
     */
    public List<Object[]> run(Connection conn, Timestamp benchmarkTimes[],
                              Timestamp startTime, Timestamp endTime) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();
        
        if (debug)
            LOG.debug(String.format("startTime=%s, endTime=%s, currentTime=%s",
                                    startTime, endTime, currentTime));

        int closed_ctr = 0;
        int waiting_ctr = 0;
        int round = AuctionMarkConstants.CLOSE_AUCTIONS_ROUNDS;
        long updated = -1;

        List<Map<String, Object>> dueItemsTable = null;
        List<Map<String, Object>> maxBidResults = null;

        final List<Object[]> output_rows = new ArrayList<Object[]>();
        while (round-- > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(AuctionMarkConstants.ITEM_COLUMNS_STR);
            sb.append(" FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" WHERE (i_start_date BETWEEN ");
            sb.append(startTime);
            sb.append(" AND ");
            sb.append(endTime);
            sb.append(") AND ");
            sb.append(ItemStatus.OPEN.ordinal());
            sb.append(" ORDER BY i_id ASC LIMIT");
            sb.append(AuctionMarkConstants.CLOSE_AUCTIONS_ITEMS_PER_ROUND);
            dueItemsTable = RestQuery.restReadQuery(sb.toString(), 0);
            if (dueItemsTable.isEmpty()) break;

            output_rows.clear();
            for (Map<String, Object> dueItemsRow : dueItemsTable) {
                long itemId = (long)dueItemsRow.get("i_id");
                long sellerId = (long)dueItemsRow.get("i_u_id");
                String i_name = (String)dueItemsRow.get("i_name");
                double currentPrice = (double)dueItemsRow.get("i_current_price");
                long numBids = (long)dueItemsRow.get("i_num_bids");
                Timestamp endDate = (Timestamp)dueItemsRow.get("i_end_date");
                ItemStatus itemStatus = ItemStatus.get((long)dueItemsRow.get("i_status"));
                Long bidId = null;
                Long buyerId = null;
                
                if (debug)
                    LOG.debug(String.format("Getting max bid for itemId=%d / sellerId=%d", itemId, sellerId));
                assert(itemStatus == ItemStatus.OPEN);
                
                // Has bid on this item - set status to WAITING_FOR_PURCHASE
                // We'll also insert a new USER_ITEM record as needed
                // We have to do this extra step because H-Store doesn't have good support in the
                // query optimizer for LEFT OUTER JOINs
                if (numBids > 0) {
                    waiting_ctr++;

                    sb = new StringBuilder();
                    sb.append("SELECT imb_ib_id, ib_buyer_id");
                    sb.append(" FROM ");
                    sb.append(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
                    sb.append(", ");
                    sb.append(AuctionMarkConstants.TABLENAME_ITEM_BID);
                    sb.append(" WHERE imb_i_id = ");
                    sb.append(itemId);
                    sb.append(" AND imb_u_id = ");
                    sb.append(sellerId);
                    sb.append(" AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id ");
                    maxBidResults = RestQuery.restReadQuery(sb.toString(), 0);
                    assert(maxBidResults != null);
                    
                    bidId = (long)maxBidResults.get(0).get("imb_ib_id");
                    buyerId = (long)maxBidResults.get(0).get("ib_buyer_id");
                    sb = new StringBuilder();
                    sb.append("INSERT INTO ");
                    sb.append(AuctionMarkConstants.TABLENAME_USERACCT_ITEM);
                    sb.append("(ui_u_id, ui_i_id, ui_i_u_id, ui_created)");
                    sb.append("VALUES(");
                    sb.append(bidId);
                    sb.append(", ");
                    sb.append(buyerId);
                    sb.append(", ");
                    sb.append(sellerId);
                    sb.append(", ");
                    sb.append(currentTime);
                    sb.append(")");
                    updated = RestQuery.restOtherQuery(sb.toString(), 0);
                    assert(updated == 1);

                    itemStatus = ItemStatus.WAITING_FOR_PURCHASE;
                }
                // No bid on this item - set status to CLOSED
                else {
                    closed_ctr++;
                    itemStatus = ItemStatus.CLOSED;
                }
                
                // Update Status!
                sb = new StringBuilder();
                sb.append("UPDATE ");
                sb.append(AuctionMarkConstants.TABLENAME_ITEM);
                sb.append(" SET i_status = ");
                sb.append(itemStatus.ordinal());
                sb.append(", i_updated = ");
                sb.append(currentTime);
                sb.append(" WHERE i_id = ");
                sb.append(itemId);
                sb.append(" AND i_u_id = ");
                sb.append(sellerId);
                updated = RestQuery.restOtherQuery(sb.toString(), 0);
                if (debug)
                    LOG.debug(String.format("Updated Status for Item %d => %s", itemId, itemStatus));
                
                Object row[] = new Object[] {
                        itemId,               // i_id
                        sellerId,             // i_u_id
                        i_name,               // i_name
                        currentPrice,         // i_current_price
                        numBids,              // i_num_bids
                        endDate,              // i_end_date
                        itemStatus.ordinal(), // i_status
                        bidId,                // imb_ib_id
                        buyerId               // ib_buyer_id
                };
                output_rows.add(row);
            } // WHILE
        } // FOR

        if (debug)
            LOG.debug(String.format("Updated Auctions - Closed=%d / Waiting=%d", closed_ctr, waiting_ctr));

        return (output_rows);
    }
}
