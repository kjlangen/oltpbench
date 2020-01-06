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

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.exceptions.DuplicateItemIdException;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.RestQuery;
import com.oltpbenchmark.util.SQLUtil;

/**
 * NewItem
 * @author pavlo
 * @author visawee
 */
public class NewItem extends Procedure {
    private static final Logger LOG = Logger.getLogger(NewItem.class);
    
	// -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
	
    public final SQLStmt insertItem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM + "(" +
        	"i_id," + 
        	"i_u_id," + 
        	"i_c_id," + 
        	"i_name," + 
        	"i_description," + 
        	"i_user_attributes," + 
        	"i_initial_price," +
        	"i_current_price," + 
        	"i_num_bids," + 
        	"i_num_images," + 
        	"i_num_global_attrs," + 
		"i_num_comments," +
        	"i_start_date," + 
        	"i_end_date," +
        	"i_status, " +
        	"i_created," +
        	"i_updated," +
        	"i_iattr0" + 
        ") VALUES (" +
            "?," +  // i_id
            "?," +  // i_u_id
            "?," +  // i_c_id
            "?," +  // i_name
            "?," +  // i_description
            "?," +  // i_user_attributes
            "?," +  // i_initial_price
            "?," +  // i_current_price
            "?," +  // i_num_bids
            "?," +  // i_num_images
            "?," +  // i_num_global_attrs
	    "0" + // i_num_comments
            "?," +  // i_start_date
            "?," +  // i_end_date
            "?," +  // i_status
            "?," +  // i_created
            "?," +  // i_updated
            "1"  +  // i_attr0
        ")"
    );
    
    public final SQLStmt getSellerItemCount = new SQLStmt(
        "SELECT COUNT(*) FROM " + AuctionMarkConstants.TABLENAME_ITEM +
        " WHERE i_u_id = ?"
    );
    
    public final SQLStmt getCategory = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_id = ? "
    );
    
    public final SQLStmt getCategoryParent = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_parent_id = ? "
    );
    
    public final SQLStmt getGlobalAttribute = new SQLStmt(
        "SELECT gag_name, gav_name, gag_c_id " +
          "FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP + ", " +
                    AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE +
        " WHERE gav_id = ? AND gav_gag_id = ? " +
           "AND gav_gag_id = gag_id"
    );
    
    public final SQLStmt insertItemAttribute = new SQLStmt(
		"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE + "(" +
			"ia_id," + 
			"ia_i_id," + 
			"ia_u_id," + 
			"ia_gav_id," + 
			"ia_gag_id" + 
		") VALUES(?, ?, ?, ?, ?)"
	);

    public final SQLStmt insertImage = new SQLStmt(
		"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_IMAGE + "(" +
			"ii_id," + 
			"ii_i_id," + 
			"ii_u_id," + 
			"ii_sattr0" + 
		") VALUES(?, ?, ?, ?)"
	);
    
    public final SQLStmt updateUserBalance = new SQLStmt(
		"UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
		   "SET u_balance = u_balance - 1, " +
		   "    u_updated = ? " +
		" WHERE u_id = ?"
	);
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
	/**
	 * Insert a new ITEM record for a user.
	 * The benchmark client provides all of the preliminary information 
	 * required for the new item, as well as optional information to create
	 * derivative image and attribute records. After inserting the new ITEM
	 * record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and
	 * ITEM IMAGE. The unique identifer for each of these records is a
	 * composite 64-bit key where the lower 60-bits are the i id parameter and the
	 * upper 4-bits are used to represent the index of the image/attribute.
	 * For example, if the i id is 100 and there are four items, then the
	 * composite key will be 0 100 for the first image, 1 100 for the second,
	 * and so on. After these records are inserted, the transaction then updates
	 * the USER record to add the listing fee to the seller's balance.
	 */
    public Object[] run(Connection conn, Timestamp benchmarkTimes[],
                        long item_id, long seller_id, long category_id,
                        String name, String description, long duration, double initial_price, String attributes,
                        long gag_ids[], long gav_ids[], String images[], int clientId) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();

	LOG.info( "Going to add new item: " + item_id );
        
        // Calculate endDate
        Timestamp end_date = new Timestamp(currentTime.getTime() + (duration * AuctionMarkConstants.MILLISECONDS_IN_A_DAY));
        StringBuilder sb = null;
        
        if (debug) {
            LOG.debug("NewItem :: run ");
            LOG.debug(">> item_id = " + item_id + " , seller_id = " + seller_id + ", category_id = " + category_id);
            LOG.debug(">> name = " + name + " , description length = " + description.length());
            LOG.debug(">> initial_price = " + initial_price + " , attributes length = " + attributes.length());
            LOG.debug(">> gag_ids[].length = " + gag_ids.length + " , gav_ids[] length = " + gav_ids.length);
            LOG.debug(">> image length = " + images.length + " ");
            LOG.debug(">> start = " + currentTime + ", end = " + end_date);
        }

        // Get attribute names and category path and append
        // them to the item description
        List<Map<String, Object>> results = null;
        long updated = -1;

	// Could certainly put a loop here.
        
        // ATTRIBUTES
        description += ";ATTRIBUTES: ";
        for (int i = 0; i < gag_ids.length; i++) {
            sb = new StringBuilder();
            sb.append("SELECT gag_name, gav_name, gag_c_id FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
            sb.append(", ");
            sb.append(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
            sb.append(" WHERE gav_id = ");
            sb.append(gav_ids[i]);
            sb.append(" AND gav_gag_id = ");
            sb.append(gag_ids[i]);
            sb.append(" AND gav_gag_id = gag_id");
            results = RestQuery.restReadQuery(sb.toString(), clientId);
            if (!results.isEmpty()) {
                description += String.format(" %s - %s,", results.get(0).get("gag_name"), results.get(0).get("gav_name"));
            }
        }
        
        // CATEGORY
        sb = new StringBuilder();
        sb.append("SELECT c_id, c_name, c_parent_id FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_CATEGORY);
        sb.append(" WHERE c_id = ");
        sb.append(category_id);
        results = RestQuery.restReadQuery(sb.toString(), clientId);
        assert(!results.isEmpty());
        String category_name = String.format("%s[%d]", results.get(0).get("c_name"), results.get(0).get("c_id"));
        
        // CATEGORY PARENT
        sb = new StringBuilder();
        sb.append("SELECT c_id, c_name, c_parent_id FROM ");
        sb.append(AuctionMarkConstants.TABLENAME_CATEGORY);
        sb.append(" WHERE c_parent_id = ");
        sb.append(category_id);
        results = RestQuery.restReadQuery(sb.toString(), clientId);
        String category_parent = null;
        if (!results.isEmpty()) {
            category_parent = String.format("%s[%d]", results.get(0).get("c_name"), results.get(0).get("c_id"));
        } else {
            category_parent = "<ROOT>";
        }
        description += String.format("CATEGORY: %s >> %s", category_parent, category_name);

        // Insert new ITEM tuple
        sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(AuctionMarkConstants.TABLENAME_ITEM);
        sb.append("(i_id, i_u_id, i_c_id, i_name, i_description, i_user_attributes, i_initial_price, ");
        sb.append("i_current_price, i_num_bids, i_num_images, i_num_global_attrs, i_num_comments, i_start_date, ");
        sb.append("i_end_date, i_status, i_created, i_updated, i_iattr0) VALUES (");
        sb.append(item_id);
        sb.append(", ");
        sb.append(seller_id);
        sb.append(", ");
        sb.append(category_id);
        sb.append(", '");
        sb.append(name);
        sb.append("', '");
        sb.append( description.replaceAll("'", "" ) );
        sb.append("', '");
        sb.append(attributes);
        sb.append("', ");
        sb.append(initial_price);
        sb.append(", ");
        sb.append(initial_price);
        sb.append(", ");
        sb.append(0);
        sb.append(", ");
        sb.append(images.length);
        sb.append(", ");
        sb.append(gav_ids.length);
        sb.append(", 0, '");
        sb.append(currentTime);
        sb.append("', '");
        sb.append(end_date);
        sb.append("', ");
        sb.append(ItemStatus.OPEN.ordinal());
        sb.append(", '");
        sb.append(currentTime);
        sb.append("', '");
        sb.append(currentTime);
        sb.append("', 1)");

        // NOTE: This may fail with a duplicate entry exception because 
        // the client's internal count of the number of items that this seller 
        // already has is wrong. That's ok. We'll just abort and ignore the problem
        // Eventually the client's internal cache will catch up with what's in the database
        // try {
        //     updated = stmt.executeUpdate();
        // } catch (SQLException ex) {
        //     if (SQLUtil.isDuplicateKeyException(ex)) {
        //         conn.rollback();
        //         results = this.getPreparedStatement(conn, getSellerItemCount, seller_id).executeQuery();
        //         adv = results.next();
        //         assert(adv);
        //         int item_count = results.getInt(1);
        //         results.close();
        //         throw new DuplicateItemIdException(item_id, seller_id, item_count, ex);
        //     } else throw ex;
        // }
        try {
            updated = RestQuery.restOtherQuery(sb.toString(), clientId);
        } catch (Exception ex) {
            sb = new StringBuilder();
            sb.append("SELECT COUNT(*) AS all_count FROM ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM);
            sb.append(" WHERE i_u_id = ");
            sb.append(seller_id);
            results = RestQuery.restReadQuery(sb.toString(), clientId);
            assert(!results.isEmpty());
            int item_count = (int)results.get(0).get("all_count");
            throw new DuplicateItemIdException(item_id, seller_id, item_count, ex);
        }
        assert(updated == 1);

        // Insert ITEM_ATTRIBUTE tuples
        for (int i = 0; i < gav_ids.length; i++) {
            sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE);
            sb.append("(ia_id, ia_i_id, ia_u_id, ia_gav_id, ia_gag_id) VALUES(");
            sb.append(AuctionMarkUtil.getUniqueElementId(item_id, i));
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", ");
            sb.append(gag_ids[i]);
            sb.append(", ");
            sb.append(gag_ids[i]);
            sb.append(")");
            updated = RestQuery.restOtherQuery(sb.toString(), clientId);
            assert(updated == 1);
        }
        
        // Insert ITEM_IMAGE tuples
        for (int i = 0; i < images.length; i++) {
            sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(AuctionMarkConstants.TABLENAME_ITEM_IMAGE);
            sb.append("(ii_id, ii_i_id, ii_u_id, ii_sattr0) VALUES(");
            sb.append(AuctionMarkUtil.getUniqueElementId(item_id, i));
            sb.append(", ");
            sb.append(item_id);
            sb.append(", ");
            sb.append(seller_id);
            sb.append(", '");
            sb.append(images[i]);
            sb.append("')");
            updated = RestQuery.restOtherQuery(sb.toString(), clientId);
            assert(updated == 1);
        }

        // Update listing fee
        sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(AuctionMarkConstants.TABLENAME_USERACCT);
        sb.append(" SET u_balance = u_balance - 1, u_updated = '");
        sb.append(currentTime);
        sb.append("' WHERE u_id = ");
        sb.append(seller_id);
	try {
		updated = RestQuery.restOtherQuery(sb.toString(), clientId);
	} catch( Exception e ) {
	}
        assert(updated == 1);
        
        // Return new item_id and user_id
        return new Object[] {
            // ITEM ID
            item_id,
            // SELLER ID
            seller_id,
            // ITEM_NAME
            name,
            // CURRENT PRICE
            initial_price,
            // NUM BIDS
            0l,
            // END DATE
            end_date.getTime(),
            // STATUS
            ItemStatus.OPEN.ordinal()
        };
    }
}
