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
import java.util.LinkedList;
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
                              Timestamp startTime, Timestamp endTime, int clientId) throws SQLException {
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
        List<List<Map<String, Object>>> maxBidResults = new LinkedList<>();
	//List<List<Map<String,Object>>> endDateResults = new LinkedList<>();

        final List<Object[]> output_rows = new ArrayList<Object[]>();
        while (round-- > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(AuctionMarkConstants.ITEM_COLUMNS_STR);
            sb.append(" FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" WHERE (i_start_date BETWEEN '");
            sb.append(startTime);
            sb.append("' AND '");
            sb.append(endTime);
            sb.append("') AND i_status = ");
            sb.append(ItemStatus.OPEN.ordinal());
            sb.append(" ORDER BY i_id ASC LIMIT " );
            sb.append(AuctionMarkConstants.CLOSE_AUCTIONS_ITEMS_PER_ROUND);
            dueItemsTable = RestQuery.restReadQuery(sb.toString(), clientId);
            LOG.warn( "CLOSE AUCTION ROWS: " + dueItemsTable.size() );
            if (dueItemsTable.isEmpty()) break;

            output_rows.clear();
            for (Map<String, Object> dueItemsRow : dueItemsTable) {
                long itemId;
                if( dueItemsRow.get("i_id") instanceof Long ) {
                    itemId = (Long) dueItemsRow.get("i_id");
                } else {
                    itemId = new Long( (Integer) dueItemsRow.get("i_id") );
                }
                long sellerId;
                if( dueItemsRow.get("i_u_id") instanceof Long ) {
                    sellerId = (Long) dueItemsRow.get("i_u_id");
                } else {
                    sellerId = new Long( (Integer) dueItemsRow.get("i_u_id") );
                }
                String i_name = (String)dueItemsRow.get("i_name");
                double currentPrice = (double)dueItemsRow.get("i_current_price");
                long numBids;
                if( dueItemsRow.get("i_num_bids") instanceof Long ) {
                    numBids = (Long) dueItemsRow.get("i_num_bids");
                } else {
                    numBids = new Long( (Integer) dueItemsRow.get("i_num_bids") );
                }
                Timestamp endDate = new Timestamp( (Long) dueItemsRow.get("i_end_date") );
                ItemStatus itemStatus = ItemStatus.get((Integer) dueItemsRow.get("i_status"));
                Long bidId = null;
                Long buyerId = null;
                
                if (debug)
                    LOG.debug(String.format("Getting max bid for itemId=%d / sellerId=%d", itemId, sellerId));
                assert(itemStatus == ItemStatus.OPEN);
                
                // Has bid on this item - set status to WAITING_FOR_PURCHASE
                // We'll also insert a new USER_ITEM record as needed
                // We have to do this extra step because H-Store doesn't have good support in the
                // query optimizer for LEFT OUTER JOINs
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
                sb.append(" AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id");
		List<Map<String,Object>> tmpResults = RestQuery.restReadQuery(sb.toString(), clientId);
		maxBidResults.add( tmpResults );

		itemStatus = tmpResults.isEmpty() ? ItemStatus.CLOSED : ItemStatus.WAITING_FOR_PURCHASE;

		if( !tmpResults.isEmpty() ) {
			Map<String,Object> row = tmpResults.get( 0 );
			if( row.get( "imb_ib_id" ) instanceof Long ) {
				bidId = (Long) row.get( "imb_ib_id" );
			} else {
				bidId = new Long( (Integer) row.get( "imb_ib_id" ) );
			}
			if( row.get( "ib_buyer_id" ) instanceof Long ) {
				buyerId = (Long) row.get( "ib_buyer_id" );
			} else {
				buyerId = new Long( (Integer) row.get( "ib_buyer_id" ) );
			}
		}

		Object row[] = new Object[] {
			itemId,               // i_id
				sellerId,             // i_u_id
				i_name,               // i_name
				currentPrice,         // i_current_price
				numBids,              // i_num_bids
				endDate.getTime(),              // i_end_date
				itemStatus.ordinal(), // i_status
				bidId,                // imb_ib_id
				buyerId               // ib_buyer_id
		};
		output_rows.add(row);

		if( !tmpResults.isEmpty() ) {

			// Retrieve the feedback for the buying user from the last 30 days
			sb = new StringBuilder();
			sb.append( "SELECT uf_u_id, uf_rating FROM " );
			sb.append( AuctionMarkConstants.TABLENAME_USERACCT_FEEDBACK );
			sb.append( " WHERE uf_u_id = " );
			sb.append( sellerId );
			sb.append( " AND uf_date > '" );
			Timestamp thirtyDays = new Timestamp(currentTime.getTime() - 2592000000L);
			sb.append( thirtyDays );
			sb.append( "'" );

			List<Map<String,Object>> tmpResults2 = RestQuery.restReadQuery(sb.toString(), clientId);

			// Do something with this?
		}

	    } // for dueItems Table
	    for( int i = 0; i < dueItemsTable.size(); i++ ) {
		    Map<String,Object> dueItemsRow = dueItemsTable.get( i );
		    List<Map<String,Object>> maxBidResultSet = maxBidResults.get( i );
		    //List<Map<String,Object>> endDateResultSet = endDateResults.get( i );

		    long itemId;
		    if( dueItemsRow.get("i_id") instanceof Long ) {
			    itemId = (Long) dueItemsRow.get("i_id");
		    } else {
			    itemId = new Long( (Integer) dueItemsRow.get("i_id") );
		    }
		    long sellerId;
		    if( dueItemsRow.get("i_u_id") instanceof Long ) {
			    sellerId = (Long) dueItemsRow.get("i_u_id");
		    } else {
			    sellerId = new Long( (Integer) dueItemsRow.get("i_u_id") );
		    }
		    double currentPrice = (double)dueItemsRow.get("i_current_price");

		    Long bidId = null;
		    Long buyerId = null;
		
		    if( !maxBidResultSet.isEmpty() /*&& !endDateResultSet.isEmpty() */ ) {

			    if( maxBidResultSet.get( 0 ).get("imb_ib_id") instanceof Long ) {
				    bidId = (Long) maxBidResultSet.get(0).get("imb_ib_id");
			    } else { 
				    bidId = new Long( (Integer) maxBidResultSet.get(0).get("imb_ib_id") );
			    }

			    if( maxBidResultSet.get(0).get("ib_buyer_id") instanceof Long ) {
				    buyerId = (Long) maxBidResultSet.get(0).get("ib_buyer_id");
			    } else {
				    buyerId = new Long( (Integer) maxBidResultSet.get(0).get("ib_buyer_id") );
			    }

			    sb = new StringBuilder();
			    sb.append("INSERT INTO ");
			    sb.append(AuctionMarkConstants.TABLENAME_USERACCT_ITEM);
			    sb.append("(ui_u_id, ui_i_id, ui_i_u_id, ui_created)");
			    sb.append("VALUES(");
			    sb.append(buyerId);
			    sb.append(", ");
			    sb.append(itemId);
			    sb.append(", ");
			    sb.append(sellerId);
			    sb.append(", '");
			    sb.append(currentTime);
			    sb.append("')");
			    try {
				    updated = RestQuery.restOtherQuery(sb.toString(), clientId);
			    } catch( Exception e ) {
				    assert(updated == 1);
			    }

		    }
		    // No bid on this item - set status to CLOSED

	    } // for dueItems/maxBids

            sb = new StringBuilder();
            sb.append("UPDATE ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" SET i_status = (CASE");
            sb.append(" WHEN i_num_bids > 0 THEN 2 ELSE 3 END)," );
            sb.append(" i_updated = '");
            sb.append(currentTime);
            sb.append("' WHERE i_id IN (");
            boolean isFirst = true;
	    for( int i = 0; i < dueItemsTable.size(); i++ ) {
		Map<String,Object> dueItemsRow = dueItemsTable.get(i);
		//List<Map<String,Object>> endDateResultSet = endDateResults.get(i);
		//if( !endDateResultSet.isEmpty() ) {
			long itemId;
			if( dueItemsRow.get("i_id") instanceof Long ) {
			    itemId = (Long) dueItemsRow.get("i_id");
			} else {
			    itemId = new Long( (Integer) dueItemsRow.get("i_id") );
			}
			if( !isFirst ) {
			    sb.append( "," );
			} else {
			    isFirst = false;
			}
			sb.append( " " );
			sb.append(itemId);
		//}
            }
            sb.append( ")" );
	    if( !isFirst ) {
		    updated = RestQuery.restOtherQuery(sb.toString(), clientId);
	    }
        } // FOR

        return (output_rows);
    }
}
