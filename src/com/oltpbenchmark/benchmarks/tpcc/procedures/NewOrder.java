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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.oltpbenchmark.api.InstrumentedSQLStmt;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.jdbc.InstrumentedPreparedStatement;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;

public class NewOrder extends Procedure {

  private static final Logger LOG = Logger.getLogger(NewOrder.class);

  private static final Initializer initializer = new Initializer();

  public static final InstrumentedSQLStmt stmtNewOrderImplSQL = new InstrumentedSQLStmt(
      "SELECT * FROM new_order_impl_ex(?, ?, ?, ?, ?, ?, ?)");

  private InstrumentedPreparedStatement stmtNewOrderImpl = null;

  public static void printLatencyStats() {
    LOG.info("NewOrder : ");
    LOG.info("latency NewOrderImpl " + stmtNewOrderImplSQL.getStats());
  }

  private static class OrderInfo {
    private final int len;
    public final Array itemIdsArray;
    public final Array warehouseArray;
    public final Array quantityArray;

    public interface ItemBuilder {
      void build(int supplierWarehouseId, int itemId, int quantity);
    }

    public OrderInfo(Connection conn, int l, Consumer<ItemBuilder> consumer) throws SQLException {
      len = l;
      ArrayList<Integer> supplierWarehouseId = new ArrayList<>();
      ArrayList<Integer> itemIds = new ArrayList<>();
      ArrayList<Integer> quantity = new ArrayList<>();

      ItemBuilder builder = (wid, iid, q) -> {
        // It is necessary to produce resulting arrays ordered by wharehouseid, itemid
        int idx = findInsertIndex(wid, iid, supplierWarehouseId, itemIds);
        supplierWarehouseId.add(idx, wid);
        itemIds.add(idx, iid);
        quantity.add(idx, q);
      };
      for (int i = 0; i < len; ++i) {
        consumer.accept(builder);
      }
      ArrayList<Integer> whs = new ArrayList<>();
      int startPos = 0;
      for (int i = 1; i < len; ++i) {
        int c_wid = supplierWarehouseId.get(startPos);
        if (supplierWarehouseId.get(i) != c_wid) {
          whs.add(c_wid);
          whs.add(startPos + 1); // index in postgres array starts from 1, so use startPos + 1 as start point
          whs.add(i);
          startPos = i;
        }
      }
      if (startPos < len) {
        whs.add(supplierWarehouseId.get(startPos));
        whs.add(startPos + 1);
        whs.add(len);
      }
      warehouseArray = conn.createArrayOf("int", whs.toArray());
      itemIdsArray = conn.createArrayOf("int", itemIds.toArray());
      quantityArray = conn.createArrayOf("int", quantity.toArray());
    }

    public int length() {
      return len;
    }

    private static int findInsertIndex(int wid, int iid, ArrayList<Integer> wids, ArrayList<Integer> iids) {
      int idx = 0;
      for (; idx < wids.size(); ++idx) {
        if (wid < wids.get(idx)) {
          return idx;
        } else if (wid == wids.get(idx)) {
          for(; idx < wids.size() && wid == wids.get(idx); ++idx) {
            if (iid <= iids.get(idx)) {
              return idx;
            }
          }
          return idx;
        }
      }
      return idx;
    }
  }

  private static class Initializer {
    private AtomicBoolean initDone = new AtomicBoolean(false);

    public void ensureInitialized(Connection conn) throws SQLException {
      if (!initDone.get()) {
        initialize(conn);
      }
    }

    private synchronized void initialize(Connection conn) throws SQLException {
      if (initDone.get()) {
        return;
      }
      LOG.info("Creating statements");
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
            "CREATE OR REPLACE PROCEDURE new_order_district_update_helper(wid INT, did INT, oid INT) AS $$\n" +
            "  UPDATE DISTRICT SET D_NEXT_O_ID = oid WHERE D_W_ID = wid AND D_ID = did;\n" +
            "$$ LANGUAGE sql;\n");

        stmt.execute(
            "CREATE OR REPLACE PROCEDURE new_order_stock_update_helper(" +
            "  iid INT, wid INT, qty numeric(4,0), ytd numeric(8,2), remote_cnt INT, order_cnt INT) AS $$\n" +
            "    UPDATE STOCK SET " +
            "      S_QUANTITY = qty," +
            "      S_YTD = ytd," +
            "      S_ORDER_CNT = order_cnt," +
            "      S_REMOTE_CNT = remote_cnt" +
            "    WHERE S_I_ID = iid AND S_W_ID = wid;\n" +
            "$$ LANGUAGE sql;");

        stmt.execute(
            "CREATE OR REPLACE FUNCTION new_order_fetch_stock(did INT, item_ids int[], wh_info int[]) RETURNS TABLE(\n" +
            "  S_I_ID INT,\n" +
            "  S_W_ID INT,\n" +
            "  S_QUANTITY NUMERIC,\n" +
            "  S_DATA VARCHAR,\n" +
            "  S_YTD NUMERIC,\n" +
            "  S_REMOTE_CNT INT,\n" +
            "  S_ORDER_CNT INT,\n" +
            "  S_DIST VARCHAR) AS $$\n" +
            "BEGIN\n" +
            "  RETURN QUERY\n" +
            "    SELECT s.S_I_ID,\n" +
            "           s.S_W_ID,\n" +
            "           s.S_QUANTITY,\n" +
            "           s.S_DATA,\n" +
            "           s.S_YTD,\n" +
            "           s.S_REMOTE_CNT,\n" +
            "           s.S_ORDER_CNT,\n" +
            "           CASE did\n" +
            "             WHEN 1 THEN CAST(s.S_DIST_01 AS VARCHAR)\n" +
            "             WHEN 2 THEN CAST(s.S_DIST_02 AS VARCHAR)\n" +
            "             WHEN 3 THEN CAST(s.S_DIST_03 AS VARCHAR)\n" +
            "             WHEN 4 THEN CAST(s.S_DIST_04 AS VARCHAR)\n" +
            "             WHEN 5 THEN CAST(s.S_DIST_05 AS VARCHAR)\n" +
            "             WHEN 6 THEN CAST(s.S_DIST_06 AS VARCHAR)\n" +
            "             WHEN 7 THEN CAST(s.S_DIST_07 AS VARCHAR)\n" +
            "             WHEN 8 THEN CAST(s.S_DIST_08 AS VARCHAR)\n" +
            "             WHEN 9 THEN CAST(s.S_DIST_09 AS VARCHAR)\n" +
            "             WHEN 10 THEN CAST(s.S_DIST_10 AS VARCHAR)\n" +
            "           ELSE\n" +
            "             NULL\n" +
            "           END S_DIST\n" +
            "    FROM (\n" +
            "      SELECT i AS w_idx, i + 1 AS s_idx, i + 2 AS e_idx FROM generate_series(1, array_length(wh_info, 1), 3) AS i\n" +
            "    ) AS g INNER JOIN stock AS s ON (wh_info[g.w_idx] = s.S_W_ID)\n" +
            "    WHERE\n" +
            "      s.S_I_ID = ANY(item_ids[wh_info[g.s_idx]:wh_info[g.e_idx]]);\n" +
            "END; $$ LANGUAGE 'plpgsql';");

        stmt.execute(
            "CREATE OR REPLACE FUNCTION new_order_for_loop_helper(\n" +
            "  idx BIGINT,\n" +
            "  quantity INT[],\n" +
            "  S_W_ID INT,\n" +
            "  I_ID INT,\n" +
            "  S_QUANTITY NUMERIC,\n" +
            "  S_REMOTE_CNT INT,\n" +
            "  S_YTD NUMERIC,\n" +
            "  S_ORDER_CNT INT,\n" +
            "  I_PRICE NUMERIC,\n" +
            "  S_DIST VARCHAR,\n" +
            "  oid INT,\n" +
            "  did INT,\n" +
            "  wid INT) RETURNS NUMERIC AS $$\n" +
            "DECLARE\n" +
            "  i_q INT;\n" +
            "  s_q NUMERIC;\n" +
            "  s_r_c INT;\n" +
            "  i_a NUMERIC;\n" +
            "BEGIN\n" +
            "  -- RAISE NOTICE 'Row %, %, %, %', idx, S_W_ID, I_ID, S_QUANTITY;\n" +
            "  i_q := quantity[idx];\n" +
            "\n" +
            "  s_q := S_QUANTITY - i_q;\n" +
            "  IF s_q < 10\n" +
            "  THEN\n" +
            "    s_q := s_q + 91;\n" +
            "  END IF;\n" +
            "\n" +
            "  s_r_c := S_REMOTE_CNT;\n" +
            "  IF wid <> S_W_ID\n" +
            "  THEN\n" +
            "    s_r_c := s_r_c + 1;\n" +
            "  END IF;\n" +
            "  CALL new_order_stock_update_helper(I_ID, S_W_ID, s_q, S_YTD + i_q, s_r_c, S_ORDER_CNT + 1); -- Trick to make single row update bufferable\n" +
            "\n" +
            "  i_a := I_PRICE * i_q;\n" +
            "  INSERT INTO ORDER_LINE\n" +
            "    (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)\n" +
            "    VALUES (oid, did, wid, idx, I_ID, S_W_ID, i_q, i_a, S_DIST);\n" +
            "  RETURN i_a;\n" +
            "END; $$ LANGUAGE 'plpgsql';");

        stmt.execute(
            "CREATE OR REPLACE FUNCTION new_order_impl_ex(wid INT, did INT, cid INT, item_ids int[], wh_info int[], quantity int[], all_local INT) RETURNS NUMERIC AS $$\n" +
            "DECLARE\n" +
            "  next_oid INT;\n" +
            "  oid INT;\n" +
            "  dtax NUMERIC;\n" +
            "  disc NUMERIC;\n" +
            "  wtax NUMERIC;\n" +
            "  amount NUMERIC;\n" +
            "  i_count NUMERIC;\n" +
            "BEGIN\n" +
            "  SELECT D_NEXT_O_ID, D_TAX INTO next_oid, dtax FROM DISTRICT WHERE D_W_ID = wid AND D_ID = did;\n" +
            "  oid := next_oid + 1;\n" +
            "  WITH cte_item AS (\n" +
            "    SELECT i.I_ID, i.I_PRICE, i.I_NAME, i.I_DATA FROM ITEM AS i WHERE i.I_ID = ANY(item_ids)\n" +
            "  ), cte_stock AS (\n" +
            "    SELECT * FROM new_order_fetch_stock(did, item_ids, wh_info)\n" +
            "  ), cte_amount AS (\n" +
            "    SELECT new_order_for_loop_helper(ROW_NUMBER () OVER (ORDER BY S_W_ID, I_ID), quantity, S_W_ID, I_ID, S_QUANTITY, S_REMOTE_CNT, S_YTD, S_ORDER_CNT, I_PRICE, S_DIST, oid, did, wid) AS value FROM cte_item INNER JOIN cte_stock ON (cte_item.I_ID = cte_stock.S_I_ID) ORDER BY S_W_ID, I_ID\n" +
            "  ) SELECT SUM(cte_amount.value), COUNT(cte_amount.value) INTO amount, i_count FROM cte_amount;\n" +
            "  IF i_count <> array_length(item_ids, 1) THEN\n" +
            "    RAISE 'User error: missed items. Requested %, but found %', array_length(item_ids, 1), i_count;\n" +
            "  END IF;\n" +
            "  CALL new_order_district_update_helper(wid, did, oid); -- Trick to make single row update bufferable\n" +
            "  INSERT INTO OORDER (O_ID, O_D_ID, O_W_ID, O_C_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (oid, did, wid, cid, array_length(item_ids, 1), all_local);\n" +
            "  INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (oid, did, wid);\n" +
            "  SELECT C_DISCOUNT INTO disc FROM CUSTOMER WHERE C_W_ID = wid AND C_D_ID = did AND C_ID = cid;\n" +
            "  SELECT W_TAX INTO wtax FROM WAREHOUSE WHERE W_ID = wid;\n" +
            "  RETURN amount * (1 + wtax + dtax) * (1 - disc);\n" +
            "END; $$ LANGUAGE 'plpgsql';");
      }
      initDone.set(true);
    }
  }

  public void run(Connection conn, Random gen,
                  int terminalWarehouseID, int numWarehouses,
                  int terminalDistrictLowerID, int terminalDistrictUpperID,
                  Worker w) throws SQLException {
    initializer.ensureInitialized(conn);
    int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
    int customerID = TPCCUtil.getCustomerID(gen);
    int numItems = TPCCUtil.randomNumber(5, 15, gen);
    AtomicBoolean all_local = new AtomicBoolean(true);
    OrderInfo order = new OrderInfo(conn, numItems, (builder) -> {
      int supplierWarehouseID = terminalWarehouseID;
      if (TPCCUtil.randomNumber(1, 100, gen) == 1) {
        do {
          supplierWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
        } while (supplierWarehouseID == terminalWarehouseID && numWarehouses > 1);
      }
      // we need to cause 1% of the new orders to be rolled back.
      // TODO -- in this case, lets make sure our retry/failure bookkeeping is smart enough to distinguish between this
      // vs. unexpected failures.
      int itemId = TPCCUtil.randomNumber(1, 100, gen) == 1 ? TPCCConfig.INVALID_ITEM_ID :  TPCCUtil.getItemID(gen);
      if (supplierWarehouseID != terminalWarehouseID) {
        all_local.set(false);
      }
      builder.build(supplierWarehouseID, itemId, TPCCUtil.randomNumber(1, 10, gen));
    });

    stmtNewOrderImpl = getPreparedStatement(conn, stmtNewOrderImplSQL);

    newOrderTransaction(conn, terminalWarehouseID, districtID, customerID, order, all_local.get());
  }

  private void newOrderTransaction(Connection conn, int w_id, int d_id, int c_id, OrderInfo order, boolean all_local) throws SQLException {
    stmtNewOrderImpl.setInt(1, w_id);
    stmtNewOrderImpl.setInt(2, d_id);
    stmtNewOrderImpl.setInt(3, c_id);
    stmtNewOrderImpl.setObject(4, order.itemIdsArray);
    stmtNewOrderImpl.setObject(5, order.warehouseArray);
    stmtNewOrderImpl.setObject(6, order.quantityArray);
    stmtNewOrderImpl.setInt(7, all_local ? 1 : 0);

    try (ResultSet r = stmtNewOrderImpl.executeQuery()) {
      r.next();
      float total_amount = r.getFloat(1);
    } catch (SQLException e) {
      if (e.getMessage().contains("User error: missed items.")) {
        throw new UserAbortException("Expected error", e);
      }
      throw e;
    }
  }
}
