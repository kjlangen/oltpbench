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
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;

/**
 * Remove ITEM entries created after the loader started
 * @author pavlo
 */
public class ResetDatabase extends Procedure {
    private static final Logger LOG = Logger.getLogger(ResetDatabase.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getLoaderStop = new SQLStmt(
        "SELECT cfp_loader_stop FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt resetItems = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_status = ?, i_updated = ?" +
        " WHERE i_status != ?" +
        "   AND i_updated > ? "
    );
    
    public final SQLStmt deleteItemPurchases = new SQLStmt(
        "DELETE FROM " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE +
        " WHERE ip_date > ?"
    );


    public void run(Connection conn) throws SQLException {
        // PreparedStatement stmt = null;
        long updated;
        
        // We have to get the loaderStopTimestamp from the CONFIG_PROFILE
        // We will then reset any changes that were made after this timestamp
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT cfp_loader_stop FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_CONFIG_PROFILE);
        List<Map<String, Object>> rs = RestQuery.restReadQuery(sb.toString(), 0);
        assert(!rs.isEmpty());
        Timestamp loaderStop = (Timestamp)rs.get(0).get("cfp_loader_stop");
        assert(loaderStop != null);

        
        // Reset ITEM information
        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append(" SET i_status = ");
        sb.append(ItemStatus.OPEN.ordinal());
        sb.append(", i_updated = ");
        sb.append(loaderStop);
        sb.append(" WHERE i_status != ");
        sb.append(ItemStatus.OPEN.ordinal());
        sb.append(" AND i_updated > ");
        sb.append(loaderStop);
        updated = RestQuery.restOtherQuery(sb.toString(), 0);
        if (LOG.isDebugEnabled())
            LOG.debug(AuctionMarkConstants.TABLENAME_ITEM + " Reset: " + updated);
        
        // Reset ITEM_PURCHASE
        sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        sb.append(" WHERE ip_date > ");
        sb.append(loaderStop);
        updated = RestQuery.restOtherQuery(sb.toString(), 0);
        if (LOG.isDebugEnabled())
            LOG.debug(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " Reset: " + updated);
    }
}
