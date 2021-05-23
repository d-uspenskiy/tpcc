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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.oltpbenchmark.api.InstrumentedSQLStmt;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.jdbc.InstrumentedPreparedStatement;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;

public class Payment extends Procedure {

  private static final Logger LOG = Logger.getLogger(Payment.class);

  private static final Initializer initializer = new Initializer();

  public static final InstrumentedSQLStmt stmtPaymentImplIdSQL = new InstrumentedSQLStmt(
      "CALL payment_impl_customer_id(?, ?, ?, ?, ?, ?)");

  public static final InstrumentedSQLStmt stmtPaymentImplNameSQL = new InstrumentedSQLStmt(
      "CALL payment_impl_customer_name(?, ?, ?, ?, ?, ?)");

  private InstrumentedPreparedStatement stmtPaymentImplId = null;
  private InstrumentedPreparedStatement stmtPaymentImplName = null;

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
            "CREATE OR REPLACE PROCEDURE payment_warehouse_update_helper(wid INT, ytd NUMERIC) AS $$\n" +
            "  UPDATE WAREHOUSE SET W_YTD = ytd WHERE W_ID = wid;\n" +
            "$$ LANGUAGE sql;");

        stmt.execute(
            "CREATE OR REPLACE PROCEDURE payment_district_update_helper(wid INT, did INT, ytd NUMERIC) AS $$\n" +
            "  UPDATE DISTRICT SET D_YTD = ytd WHERE D_W_ID = wid AND D_ID = did;\n" +
            "$$ LANGUAGE sql;");

        stmt.execute(
            "CREATE OR REPLACE PROCEDURE payment_customer_update_helper(wid INT, did INT, cid INT, bal NUMERIC, ytd DOUBLE PRECISION, cnt INT) AS $$\n" +
            "  UPDATE CUSTOMER SET C_BALANCE = bal, C_YTD_PAYMENT = ytd, C_PAYMENT_CNT = cnt WHERE C_W_ID = wid  AND C_D_ID = did AND C_ID = cid;\n" +
            "$$ LANGUAGE sql;");

        stmt.execute(
            "CREATE OR REPLACE PROCEDURE payment_customer_update_data_helper(wid INT, did INT, cid INT, bal NUMERIC, ytd DOUBLE PRECISION, cnt INT, data VARCHAR) AS $$\n" +
            "  UPDATE CUSTOMER SET C_BALANCE = bal, C_YTD_PAYMENT = ytd, C_PAYMENT_CNT = cnt, C_DATA = data WHERE C_W_ID = wid  AND C_D_ID = did AND C_ID = cid;\n" +
            "$$ LANGUAGE sql;");

        stmt.execute(
            "CREATE OR REPLACE PROCEDURE payment_impl_customer_id(wid INT, did INT, cwid INT, cdid INT, cid INT, amount DOUBLE PRECISION) AS $$\n" +
            "DECLARE\n" +
            "  wstr_1 VARCHAR;\n" +
            "  wstr_2 VARCHAR;\n" +
            "  wcity VARCHAR;\n" +
            "  wstate VARCHAR;\n" +
            "  wzip VARCHAR;\n" +
            "  wname VARCHAR;\n" +
            "  wytd NUMERIC;\n" +
            "  dstr_1 VARCHAR;\n" +
            "  dstr_2 VARCHAR;\n" +
            "  dcity VARCHAR;\n" +
            "  dstate VARCHAR;\n" +
            "  dzip VARCHAR;\n" +
            "  dname VARCHAR;\n" +
            "  dytd NUMERIC;\n" +
            "  cfirst VARCHAR;\n" +
            "  cmiddle VARCHAR;\n" +
            "  clast VARCHAR;\n" +
            "  cstr_1 VARCHAR;\n" +
            "  cstr_2 VARCHAR;\n" +
            "  ccity VARCHAR;\n" +
            "  cstate VARCHAR;\n" +
            "  czip VARCHAR;\n" +
            "  cphone VARCHAR;\n" +
            "  ccredit VARCHAR;\n" +
            "  ccredit_lim NUMERIC;\n" +
            "  cdiscount NUMERIC;\n" +
            "  cbalance NUMERIC;\n" +
            "  cytd DOUBLE PRECISION;\n" +
            "  cpayment_cnt INT;\n" +
            "  csince TIMESTAMP WITHOUT TIME ZONE;\n" +
            "  cdata VARCHAR;\n" +
            "BEGIN\n" +
            "  SELECT\n" +
            "    C_FIRST,\n" +
            "    C_MIDDLE,\n" +
            "    C_LAST,\n" +
            "    C_STREET_1,\n" +
            "    C_STREET_2,\n" +
            "    C_CITY,\n" +
            "    C_STATE,\n" +
            "    C_ZIP,\n" +
            "    C_PHONE,\n" +
            "    C_CREDIT,\n" +
            "    C_CREDIT_LIM,\n" +
            "    C_DISCOUNT,\n" +
            "    C_BALANCE,\n" +
            "    C_YTD_PAYMENT,\n" +
            "    C_PAYMENT_CNT,\n" +
            "    C_SINCE,\n" +
            "    C_DATA\n" +
            "  INTO cfirst, cmiddle, clast, cstr_1, cstr_2, ccity, cstate, czip, cphone, ccredit, ccredit_lim, cdiscount, cbalance, cytd, cpayment_cnt, csince, cdata\n" +
            "  FROM CUSTOMER WHERE C_W_ID = cwid AND C_D_ID = cdid AND C_ID = cid FOR KEY SHARE;\n" +
            "  SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME, W_YTD INTO wstr_1, wstr_2, wcity, wstate, wzip, wname, wytd FROM WAREHOUSE WHERE W_ID = wid;\n" +
            "  wytd := wytd + amount;\n" +
            "  SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME, D_YTD INTO dstr_1, dstr_2, dcity, dstate, dzip, dname, dytd FROM DISTRICT WHERE D_W_ID = wid AND D_ID = did FOR NO KEY UPDATE;\n" +
            "  dytd := dytd + amount;\n" +
            "  cbalance := cbalance - amount;\n" +
            "  cytd := cytd + amount;\n" +
            "  cpayment_cnt := cpayment_cnt + 1;\n" +
            "  IF ccredit = 'BC' THEN\n" +
            "    -- Bad balance\n" +
            "    cdata := SUBSTRING(cid || ' ' || cdid || ' ' || cwid  || ' ' || did || ' ' || wid || ' ' || amount || ' | ' || cdata, 1, 500);\n" +
            "    CALL payment_customer_update_data_helper(cwid, cdid, cid, cbalance, cytd, cpayment_cnt, cdata);\n" +
            "  ELSE\n" +
            "    -- Good balance\n" +
            "    CALL payment_customer_update_helper(cwid, cdid, cid, cbalance, cytd, cpayment_cnt);\n" +
            "  END IF;\n" +
            "  CALL payment_warehouse_update_helper(wid, wytd);\n" +
            "  CALL payment_district_update_helper(wid, did, wytd);\n" +
            "  INSERT INTO HISTORY(H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES (cdid,cwid,cid,did,wid,NOW(),amount,SUBSTRING(wname, 1, 10) || '    ' || SUBSTRING(dname, 1, 10));\n" +
            "END; $$ LANGUAGE 'plpgsql';");

        stmt.execute(
            "CREATE OR REPLACE PROCEDURE payment_impl_customer_name(wid INT, did INT, cwid INT, cdid INT, lname VARCHAR, amount DOUBLE PRECISION) AS $$\n" +
            "DECLARE\n" +
            "  wstr_1 VARCHAR;\n" +
            "  wstr_2 VARCHAR;\n" +
            "  wcity VARCHAR;\n" +
            "  wstate VARCHAR;\n" +
            "  wzip VARCHAR;\n" +
            "  wname VARCHAR;\n" +
            "  wytd NUMERIC;\n" +
            "  dstr_1 VARCHAR;\n" +
            "  dstr_2 VARCHAR;\n" +
            "  dcity VARCHAR;\n" +
            "  dstate VARCHAR;\n" +
            "  dzip VARCHAR;\n" +
            "  dname VARCHAR;\n" +
            "  dytd NUMERIC;\n" +
            "  cfirst VARCHAR;\n" +
            "  cmiddle VARCHAR;\n" +
            "  clast VARCHAR;\n" +
            "  cstr_1 VARCHAR;\n" +
            "  cstr_2 VARCHAR;\n" +
            "  ccity VARCHAR;\n" +
            "  cstate VARCHAR;\n" +
            "  czip VARCHAR;\n" +
            "  cphone VARCHAR;\n" +
            "  ccredit VARCHAR;\n" +
            "  ccredit_lim NUMERIC;\n" +
            "  cdiscount NUMERIC;\n" +
            "  cbalance NUMERIC;\n" +
            "  cytd DOUBLE PRECISION;\n" +
            "  cpayment_cnt INT;\n" +
            "  csince TIMESTAMP WITHOUT TIME ZONE;\n" +
            "  cdata VARCHAR;\n" +
            "  cid INT;\n" +
            "BEGIN\n" +
            "  SELECT\n" +
            "    C_FIRST,\n" +
            "    C_MIDDLE,\n" +
            "    C_LAST,\n" +
            "    C_STREET_1,\n" +
            "    C_STREET_2,\n" +
            "    C_CITY,\n" +
            "    C_STATE,\n" +
            "    C_ZIP,\n" +
            "    C_PHONE,\n" +
            "    C_CREDIT,\n" +
            "    C_CREDIT_LIM,\n" +
            "    C_DISCOUNT,\n" +
            "    C_BALANCE,\n" +
            "    C_YTD_PAYMENT,\n" +
            "    C_PAYMENT_CNT,\n" +
            "    C_SINCE,\n" +
            "    C_DATA,\n" +
            "    C_ID\n" +
            "  INTO cfirst, cmiddle, clast, cstr_1, cstr_2, ccity, cstate, czip, cphone, ccredit, ccredit_lim, cdiscount, cbalance, cytd, cpayment_cnt, csince, cdata, cid\n" +
            "  FROM CUSTOMER WHERE C_W_ID = cwid AND C_D_ID = cdid AND C_LAST = lname ORDER BY C_FIRST LIMIT 1 FOR KEY SHARE;\n" +
            "  SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME, W_YTD INTO wstr_1, wstr_2, wcity, wstate, wzip, wname, wytd FROM WAREHOUSE WHERE W_ID = wid;\n" +
            "  wytd := wytd + amount;\n" +
            "  SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME, D_YTD INTO dstr_1, dstr_2, dcity, dstate, dzip, dname, dytd FROM DISTRICT WHERE D_W_ID = wid AND D_ID = did FOR NO KEY UPDATE;\n" +
            "  dytd := dytd + amount;\n" +
            "  cbalance := cbalance - amount;\n" +
            "  cytd := cytd + amount;\n" +
            "  cpayment_cnt := cpayment_cnt + 1;\n" +
            "  IF ccredit = 'BC' THEN\n" +
            "    -- Bad balance\n" +
            "    cdata := SUBSTRING(cid || ' ' || cdid || ' ' || cwid  || ' ' || did || ' ' || wid || ' ' || amount || ' | ' || cdata, 1, 500);\n" +
            "    CALL payment_customer_update_data_helper(cwid, cdid, cid, cbalance, cytd, cpayment_cnt, cdata);\n" +
            "  ELSE\n" +
            "    -- Good balance\n" +
            "    CALL payment_customer_update_helper(cwid, cdid, cid, cbalance, cytd, cpayment_cnt);\n" +
            "  END IF;\n" +
            "  CALL payment_warehouse_update_helper(wid, wytd);\n" +
            "  CALL payment_district_update_helper(wid, did, wytd);\n" +
            "  INSERT INTO HISTORY(H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES (cdid,cwid,cid,did,wid,NOW(),amount,SUBSTRING(wname, 1, 10) || '    ' || SUBSTRING(dname, 1, 10));\n" +
            "END; $$ LANGUAGE 'plpgsql';");
      }
      initDone.set(true);
    }
  }

  public static void printLatencyStats() {
    LOG.info("Payment : ");
    LOG.info("latency PaymentImplId " + stmtPaymentImplIdSQL.getStats());
    LOG.info("latency PaymentImplName " + stmtPaymentImplNameSQL.getStats());
  }

  public void run(Connection conn, Random gen,
                  int w_id, int numWarehouses,
                  int terminalDistrictLowerID, int terminalDistrictUpperID,
                  Worker w) throws SQLException {
    initializer.ensureInitialized(conn);
    stmtPaymentImplId = getPreparedStatement(conn, stmtPaymentImplIdSQL);
    stmtPaymentImplName = getPreparedStatement(conn, stmtPaymentImplNameSQL);

    int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

    int x = TPCCUtil.randomNumber(1, 100, gen);
    int customerDistrictID;
    int customerWarehouseID;
    if (x <= 85) {
      customerDistrictID = districtID;
      customerWarehouseID = w_id;
    } else {
      customerDistrictID = TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
      do {
          customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
      } while (customerWarehouseID == w_id && numWarehouses > 1);
    }

    float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

    if (TPCCUtil.randomNumber(1, 100, gen) <= 60) {
      // 60% lookups by last name
      String customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
      stmtPaymentImplName.setInt(1, w_id);
      stmtPaymentImplName.setInt(2, districtID);
      stmtPaymentImplName.setInt(3, customerWarehouseID);
      stmtPaymentImplName.setInt(4, customerDistrictID);
      stmtPaymentImplName.setString(5, customerLastName);
      stmtPaymentImplName.setDouble(6, paymentAmount);
      stmtPaymentImplName.execute();
    } else {
      // 40% lookups by customer ID
      int customerID = TPCCUtil.getCustomerID(gen);
      stmtPaymentImplId.setInt(1, w_id);
      stmtPaymentImplId.setInt(2, districtID);
      stmtPaymentImplId.setInt(3, customerWarehouseID);
      stmtPaymentImplId.setInt(4, customerDistrictID);
      stmtPaymentImplId.setInt(5, customerID);
      stmtPaymentImplId.setDouble(6, paymentAmount);
      stmtPaymentImplId.execute();
    }
  }
}
