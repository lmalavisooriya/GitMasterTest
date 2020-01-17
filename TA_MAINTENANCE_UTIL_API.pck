CREATE OR REPLACE PACKAGE TA_MAINTENANCE_UTIL_API IS

module_  CONSTANT VARCHAR2(25) := 'TAUTIL';
lu_name_ CONSTANT VARCHAR2(25) := 'TaMaintenanceUtil';


PROCEDURE MT1_Cleanup_Inventory;
PROCEDURE MT2_Rem_Purch_Req_No_Lines;
PROCEDURE MT3_Upd_Inv_Part_Acc_Grp_ASMOD;
PROCEDURE MT4_Upd_Inv_Part_Acc_Grp;
PROCEDURE MT5_Upd_Inv_ANU_Part_Stat;
PROCEDURE MT6_Upd_Inv_Part_Stat;
PROCEDURE MT7_Upd_Inv_Part_Stat_Asset;
PROCEDURE MT8_Upd_Inv_Part_Planning;
PROCEDURE MT9_Upd_Purch_Part_Supp_Stat;
PROCEDURE Mt10_Upd_Purch_Part_Supp_Uom;
PROCEDURE MT11_Upd_Inv_Part_Zero_Cost_1;
PROCEDURE MT11_Upd_Inv_Part_Zero_Cost_2;
PROCEDURE Mt12_Upd_Inv_Part_Tariff_Code;
PROCEDURE MT13_Upd_Purch_Line_Price;
PROCEDURE MT14_Upd_Purch_Line_Recpt_Date;
PROCEDURE MT15_Upd_Purch_Part_Supp_Curr;
PROCEDURE MT16_Upd_Purch_Part_Supp_Price;
PROCEDURE Mt17_Upd_Purch_Part_Supp_Part (
   limit_    IN NUMBER,
   rollback_ IN VARCHAR2 DEFAULT 'NO' );
PROCEDURE Mt17_Upd_Pps_Vendor_Part_Bkg (
   objid_      IN VARCHAR2,
   objver_     IN VARCHAR2,
   contract_   IN VARCHAR2,
   part_no_    IN VARCHAR2,
   vendor_     IN VARCHAR2,
   start_time_ IN DATE     DEFAULT null,
   job_no_     IN NUMBER   DEFAULT null,
   job_total_  IN NUMBER   DEFAULT null,
   rollback_   IN VARCHAR2 DEFAULT 'NO' );
PROCEDURE  MT18_Upd_Sales_Part_Sales_Grp;
PROCEDURE Mt19_Upd_Pps_Interco_Price (
   limit_    IN NUMBER,
   part_no_  IN VARCHAR2 DEFAULT '%',
   rollback_ IN VARCHAR2 DEFAULT 'NO' );
PROCEDURE Mt19_Upd_Pps_Price_Bkg (
   objid_      IN VARCHAR2,
   objver_     IN VARCHAR2,
   contract_   IN VARCHAR2,
   part_no_    IN VARCHAR2,
   vendor_     IN VARCHAR2,
   inv_value_  IN NUMBER,
   start_time_ IN DATE     DEFAULT null,
   job_no_     IN NUMBER   DEFAULT null,
   job_total_  IN NUMBER   DEFAULT null,
   rollback_   IN VARCHAR2 DEFAULT 'NO' );
PROCEDURE  MT20_Upd_Sales_Price;
PROCEDURE  MT21_Upd_Inv_Part_Est_Mat_Cost;
PROCEDURE  Log_Error (
   method_name_    IN VARCHAR2,
   error_code_     IN VARCHAR2,
   error_message_  IN VARCHAR2,
   key_values_     IN VARCHAR2,
   log_time_       IN DATE DEFAULT sysdate );
PROCEDURE  Clear_Error_Log;

END TA_MAINTENANCE_UTIL_API;
/
CREATE OR REPLACE PACKAGE BODY TA_MAINTENANCE_UTIL_API IS

------------------------------------------------------------------------------
-- Logging helpers
--
PROCEDURE Inform_User (
   job_name_   IN VARCHAR2,
   start_time_ IN DATE,
   job_no_     IN NUMBER,
   job_total_  IN NUMBER )
IS
   runtime_     NUMBER       := sysdate - start_time_;
   runtime_txt_ VARCHAR2(30) := numtodsinterval (runtime_, 'DAY');
   title_       VARCHAR2(2000);
   msg_         VARCHAR2(4000);
BEGIN
   IF start_time_ IS not null THEN
      runtime_txt_ := ltrim (runtime_txt_, '+0');
      runtime_txt_ := ltrim (runtime_txt_, ' ');
      runtime_txt_ := substr (runtime_txt_, 1, length(runtime_txt_)-10);
      runtime_txt_ := replace (runtime_txt_, ' ', ' days ');
      IF job_no_ = 1 THEN
         title_ := Cbh_Utils_API.Text_Replace (':P1 update job started', job_name_);
         msg_   := Cbh_Utils_API.Text_Replace (
            '<p>:P1 update job started at <strong>:P2</strong></p>' ||
            '<p>This job is scheduled to update :P3 records.</p>'   ||
            '<p>You will receive a second e-mail when the final part is updated.</p>',
            job_name_, to_char(start_time_, 'YYYY-MM-DD HH24:MI:SS'), to_char(job_total_)
         );
      ELSIF job_no_ = job_total_ THEN
         title_ := Cbh_Utils_API.Text_Replace (':P1 update job finished', job_name_);
         msg_   := Cbh_Utils_API.Text_Replace (
            '<p>:P1 update job finished at <strong>:P2</strong></p>'                     ||
            '<p>It started at <strong>:P3</strong> and ran for <strong>:P4</strong></p>' ||
            '<p>A total of <strong>:P5</strong> records were updated.</p>',
            job_name_, to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS'),
            to_char(start_time_, 'YYYY-MM-DD HH24:MI:SS'), runtime_txt_, to_char(job_total_)
         );
      END IF;
      IF title_ IS not null THEN
         Ser_Html_Mail_API.Send_Mail (
            user_id_or_email_ => 'IFSAPP',
            from_name_        => 'IFS Applications',
            to_commalist_     => 'OGARTH,OLENOIR',
            cc_commalist_     => '',
            bcc_commalist_    => '',
            subject_          => title_,
            mail_html_        => msg_
         );
      END IF;
   END IF;
END Inform_User;


------------------------------------------------------------------------------
-- PUBLIC METHOD DECLARATIONS
--

PROCEDURE  MT1_Cleanup_Inventory
IS
   message_    VARCHAR2(3000);
   key_values_ VARCHAR2(1000);

   CURSOR get_sites_with_zero_val IS
   SELECT CONTRACT
     FROM INVENTORY_VALUE_PART_SUM_EXT
    WHERE to_char(STAT_YEAR_NO) || lpad(to_char(STAT_PERIOD_NO),2,'0') = to_char(sysdate, 'YYYYMM')
 GROUP BY CONTRACT
   HAVING floor(sum(TOTAL_VALUE)) <= 0;

BEGIN
   FOR site_ IN get_sites_with_zero_val LOOP
     Message_sys.Add_Attribute(message_, 'SITE', site_.CONTRACT);
     Message_sys.Add_Attribute(message_, 'PROJECT_ID', '%');
     Message_sys.Add_Attribute(message_, 'NUMBER_OF_DAYS', 0);
     Message_sys.Add_Attribute(message_, 'SERIALS_ONLY', 'FALSE');
     Message_sys.Add_Attribute(message_, 'CONFIGURATIONS_ONLY', 'FALSE');

     BEGIN
          --Batch_Schedule_Method_API.Execute_Online__(82, message_);
          Cleanup_Inventory_API.Cleanup_Routine(message_);
          EXCEPTION
            WHEN OTHERS THEN
             Log_Error ('MT1_Cleanup_Inventory', SQLCODE, SQLERRM, key_values_);
     END;
   END LOOP;
END MT1_Cleanup_Inventory;

--Delete purchase requistion header if there aren't any requisition lines connected
PROCEDURE  MT2_Rem_Purch_Req_No_Lines
IS
  CURSOR get_pur_reqs IS
   SELECT requisition_no
   FROM PURCHASE_REQUISITION;

  CURSOR get_req_line(requisition_no_ IN VARCHAR2) IS
   SELECT 'X'
   FROM PURCHASE_REQ_LINE_ALL
   WHERE requisition_no = requisition_no_;

  dummy_ VARCHAR2(1);
  key_values_ VARCHAR2(1000);
BEGIN
  FOR pur_req_ IN get_pur_reqs LOOP
   OPEN get_req_line(pur_req_.requisition_no);
   FETCH get_req_line INTO dummy_;
   IF (get_req_line%NOTFOUND) THEN
      CLOSE get_req_line;
      BEGIN
        Purchase_Requisition_Api.Remove(pur_req_.requisition_no);
        EXCEPTION
          WHEN OTHERS THEN
           key_values_ := 'REQUISITION_NO: '||pur_req_.requisition_no;
           Log_Error ('MT2_Rem_Purch_Req_No_Lines', SQLCODE, SQLERRM, key_values_);
      END;
   ELSE
     CLOSE get_req_line;
   END IF;
  END LOOP;
END MT2_Rem_Purch_Req_No_Lines;

PROCEDURE  MT3_Upd_Inv_Part_Acc_Grp_ASMOD
IS
   CURSOR get_inv_parts IS
   SELECT part_no, contract, accounting_group, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_tab
    WHERE accounting_group NOT IN ('ASMOD');

   part_cat_main_grp_ Part_Catalog_Tab.Part_Main_Group%TYPE;
   info_              VARCHAR2(2000);
   objid_             VARCHAR2(2000);
   objversion_        VARCHAR2(2000);
   attr_              VARCHAR2(2000);
   key_values_        VARCHAR2(1000);

BEGIN
  FOR inv_part_ IN get_inv_parts LOOP
    part_cat_main_grp_ := Part_Catalog_API.Get_Part_Main_Group(inv_part_.part_no);
    BEGIN
      IF (part_cat_main_grp_ = 'M') THEN
        objid_ := inv_part_.rowid;
        objversion_ := inv_part_.objvers;

        client_sys.Clear_Attr(attr_);
        client_sys.Add_To_Attr('ACCOUNTING_GROUP','ASMOD',attr_);

        Inventory_Part_API.Modify__(info_,objid_,objversion_,attr_,'DO');
      END IF;
      EXCEPTION
        WHEN OTHERS THEN
           key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
             Log_Error ('MT3_Upd_Inv_Part_Acc_Grp_ASMOD', SQLCODE, SQLERRM, key_values_);
    END;
  END LOOP;
END MT3_Upd_Inv_Part_Acc_Grp_ASMOD;

PROCEDURE  MT4_Upd_Inv_Part_Acc_Grp
IS
   CURSOR get_inv_parts IS
   SELECT part_no, contract, accounting_group, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_tab
    WHERE contract NOT IN ('GLO1X');

   acc_grp_in_glo1x_  VARCHAR2(100);
   info_              VARCHAR2(2000);
   objid_             VARCHAR2(2000);
   objversion_        VARCHAR2(2000);
   attr_              VARCHAR2(2000);
   key_values_        VARCHAR2(1000);
BEGIN
   FOR inv_part_ IN get_inv_parts LOOP
      acc_grp_in_glo1x_ := Inventory_Part_Api.Get_Accounting_Group('GLO1X', inv_part_.part_no);
      IF (acc_grp_in_glo1x_ != inv_part_.accounting_group) THEN
        client_sys.Clear_Attr(attr_);
        client_sys.Add_To_Attr('ACCOUNTING_GROUP',acc_grp_in_glo1x_,attr_);
        objid_      := inv_part_.rowid;
        objversion_ := inv_part_.objvers;
        BEGIN
           Inventory_Part_API.Modify__(info_,objid_,objversion_,attr_,'DO');
        EXCEPTION
           WHEN OTHERS THEN
              key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
             Log_Error ('MT4_Upd_Inv_Part_Acc_Grp', SQLCODE, SQLERRM, key_values_);
        END;
      END IF;
   END LOOP;
END MT4_Upd_Inv_Part_Acc_Grp;

PROCEDURE  MT5_Upd_Inv_ANU_Part_Stat
IS
  CURSOR get_inv_parts IS
   SELECT part_no, contract, accounting_group, part_status, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_tab
    WHERE contract IN ('GLO1X')
      AND part_no LIKE 'ANU%'
      AND accounting_group NOT IN ('ASSET');


   ann_part_id_    INVENTORY_PART_TAB.PART_NO%TYPE;
   anu_part_stat_  INVENTORY_PART_TAB.PART_STATUS%TYPE;
   ann_part_stat_  INVENTORY_PART_TAB.PART_STATUS%TYPE;

   info_              VARCHAR2(2000);
   objid_             VARCHAR2(2000);
   objversion_        VARCHAR2(2000);
   attr_              VARCHAR2(2000);
   key_values_        VARCHAR2(1000);
BEGIN
   FOR inv_part_ IN get_inv_parts LOOP
      ann_part_id_ := CONCAT('ANN',LTRIM(inv_part_.part_no,'ANU'));
      anu_part_stat_ := inv_part_.part_status;
      ann_part_stat_ := Inventory_part_API.Get_Part_Status('GLO1X', ann_part_id_);
      IF (anu_part_stat_ != ann_part_stat_) THEN
        client_sys.Clear_Attr(attr_);
        client_sys.Add_To_Attr('PART_STATUS',ann_part_stat_,attr_);
        objid_      := inv_part_.rowid;
        objversion_ := inv_part_.objvers;
        BEGIN
           Inventory_Part_API.Modify__(info_,objid_,objversion_,attr_,'DO');
        EXCEPTION
           WHEN OTHERS THEN
               key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
             Log_Error ('MT5_Upd_Inv_ANU_Part_Stat', SQLCODE, SQLERRM, key_values_);
        END;
      END IF;
   END LOOP;
END MT5_Upd_Inv_ANU_Part_Stat;

PROCEDURE  MT6_Upd_Inv_Part_Stat
IS
   CURSOR get_inv_parts IS
   SELECT part_no, contract, accounting_group, part_status, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_tab
    WHERE contract NOT IN ('GLO1X')
      AND accounting_group NOT IN ('ASSET');

    glo1x_inv_part_stat_   VARCHAR2(10);
    info_                  VARCHAR2(2000);
    objid_                 VARCHAR2(2000);
    objversion_            VARCHAR2(2000);
    attr_                  VARCHAR2(2000);
    key_values_            VARCHAR2(1000);
BEGIN
   FOR inv_part_ IN get_inv_parts LOOP
      glo1x_inv_part_stat_ := Inventory_part_API.Get_Part_Status('GLO1X', inv_part_.part_no);
      IF ((glo1x_inv_part_stat_ != inv_part_.part_status) AND (glo1x_inv_part_stat_ IN ('A', 'B', 'C', 'D', 'E', 'F', 'G'))) THEN
        client_sys.Clear_Attr(attr_);
        client_sys.Add_To_Attr('PART_STATUS',glo1x_inv_part_stat_, attr_);
        objid_      := inv_part_.rowid;
        objversion_ := inv_part_.objvers;
        BEGIN
           Inventory_Part_API.Modify__(info_,objid_,objversion_,attr_,'DO');
        EXCEPTION
           WHEN OTHERS THEN
             key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
             Log_Error ('MT6_Upd_Inv_Part_Stat', SQLCODE, SQLERRM, key_values_);
        END;
      END IF;
   END LOOP;
END MT6_Upd_Inv_Part_Stat;

PROCEDURE  MT7_Upd_Inv_Part_Stat_Asset
IS
  CURSOR get_inv_parts IS
   SELECT part_no, contract, accounting_group, part_status, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_tab
    WHERE contract NOT IN ('GLO1X')
      AND accounting_group IN ('ASSET');

    glo1x_inv_part_stat_   VARCHAR2(10);
    info_                  VARCHAR2(2000);
    objid_                 VARCHAR2(2000);
    objversion_            VARCHAR2(2000);
    attr_                  VARCHAR2(2000);
    key_values_            VARCHAR2(1000);
BEGIN
   FOR inv_part_ IN get_inv_parts LOOP
      glo1x_inv_part_stat_ := Inventory_part_API.Get_Part_Status('GLO1X', inv_part_.part_no);
      IF ((glo1x_inv_part_stat_ != inv_part_.part_status) AND (glo1x_inv_part_stat_ IN ('A', 'E', 'S'))) THEN
        client_sys.Clear_Attr(attr_);
        client_sys.Add_To_Attr('PART_STATUS',glo1x_inv_part_stat_, attr_);
        objid_      := inv_part_.rowid;
        objversion_ := inv_part_.objvers;
        BEGIN
           Inventory_Part_API.Modify__(info_,objid_,objversion_,attr_,'DO');
        EXCEPTION
           WHEN OTHERS THEN
              key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
              Log_Error ('MT7_Upd_Inv_Part_Stat_Asset', SQLCODE, SQLERRM, key_values_);
        END;
      END IF;
   END LOOP;
END MT7_Upd_Inv_Part_Stat_Asset;

PROCEDURE MT8_Upd_Inv_Part_Planning
IS
  CURSOR get_inv_parts IS
  SELECT OBJID,
         OBJVERSION,
         CONTRACT,
         PART_NO,
         LOT_SIZE || LOT_SIZE_AUTO_DB || MAXWEEK_SUPPLY || MAX_ORDER_QTY || MIN_ORDER_QTY || MUL_ORDER_QTY || ORDER_POINT_QTY || ORDER_POINT_QTY_AUTO_DB || SAFETY_STOCK || SAFETY_STOCK_AUTO_DB || SERVICE_RATE || SETUP_COST || STD_ORDER_SIZE || ORDER_REQUISITION_DB || PLANNING_METHOD || PLANNING_METHOD_AUTO_DB
    FROM INVENTORY_PART_PLANNING
   WHERE INVENTORY_PART_API.Get_Part_Status(CONTRACT, PART_NO) != 'A'
     AND Inventory_Part_API.Get_Accounting_Group(CONTRACT, PART_NO) != 'ASSET'
     AND LOT_SIZE || LOT_SIZE_AUTO_DB || MAXWEEK_SUPPLY || MAX_ORDER_QTY || MIN_ORDER_QTY || MUL_ORDER_QTY || ORDER_POINT_QTY || ORDER_POINT_QTY_AUTO_DB || SAFETY_STOCK || SAFETY_STOCK_AUTO_DB || SERVICE_RATE || SETUP_COST || STD_ORDER_SIZE || ORDER_REQUISITION_DB || PLANNING_METHOD || PLANNING_METHOD_AUTO_DB != '0N00010N0N5000RAFALSE';

  CURSOR get_inv_parts2 IS
  SELECT OBJID,
         OBJVERSION,
         CONTRACT,
         PART_NO
    FROM INVENTORY_PART_PLANNING
   WHERE PLANNING_METHOD_AUTO_DB = 'TRUE';

    info_                  VARCHAR2(2000);
    attr_                  VARCHAR2(2000);
    key_values_            VARCHAR2(1000);
BEGIN
      FOR inv_part_ IN get_inv_parts LOOP
        Client_SYS.Clear_Attr (attr_);
        Client_SYS.Add_To_Attr ('LOT_SIZE', 0, attr_);
        Client_SYS.Add_To_Attr ('LOT_SIZE_AUTO_DB', 'N', attr_);
        Client_SYS.Add_To_Attr ('MAXWEEK_SUPPLY', 0, attr_);
        Client_SYS.Add_To_Attr ('MAX_ORDER_QTY', 0, attr_);
        Client_SYS.Add_To_Attr ('MIN_ORDER_QTY', 0, attr_);
        Client_SYS.Add_To_Attr ('MUL_ORDER_QTY', 1, attr_);
        Client_SYS.Add_To_Attr ('ORDER_POINT_QTY', 0, attr_);
        Client_SYS.Add_To_Attr ('ORDER_POINT_QTY_AUTO_DB', 'N', attr_);
        Client_SYS.Add_To_Attr ('SAFETY_STOCK', 0, attr_);
        Client_SYS.Add_To_Attr ('SAFETY_STOCK_AUTO_DB', 'N', attr_);
        Client_SYS.Add_To_Attr ('SERVICE_RATE', 50, attr_);
        Client_SYS.Add_To_Attr ('SETUP_COST', 0, attr_);
        Client_SYS.Add_To_Attr ('STD_ORDER_SIZE', 0, attr_);
        Client_SYS.Add_To_Attr ('ORDER_REQUISITION_DB', 'R', attr_);
        Client_SYS.Add_To_Attr ('PLANNING_METHOD', 'A', attr_);
        Client_SYS.Add_To_Attr ('PLANNING_METHOD_AUTO_DB', 'FALSE', attr_);
        BEGIN
         Inventory_Part_Planning_API.Modify__(info_, inv_part_.OBJID, inv_part_.OBJVERSION, attr_, 'DO');
        EXCEPTION
          WHEN OTHERS THEN
            key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
            Log_Error ('MT8_Upd_Inv_Part_Planning', SQLCODE, SQLERRM, key_values_);
        END;
      END LOOP;
      COMMIT;

    FOR inv_part_ IN get_inv_parts2 LOOP
        Client_SYS.Clear_Attr (attr_);
        Client_SYS.Add_To_Attr ('PLANNING_METHOD_AUTO_DB', 'FALSE', attr_);
        BEGIN
         Inventory_Part_Planning_API.Modify__(info_, inv_part_.OBJID, inv_part_.OBJVERSION, attr_, 'DO');
        EXCEPTION
          WHEN OTHERS THEN
            key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
            Log_Error ('MT8_Upd_Inv_Part_Planning', SQLCODE, SQLERRM, key_values_);
        END;
    END LOOP;
    COMMIT;
END MT8_Upd_Inv_Part_Planning;


PROCEDURE  MT9_Upd_Purch_Part_Supp_Stat
IS
 CURSOR get_pur_part_supplier IS
 SELECT part_no, contract, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
   FROM PURCHASE_PART_SUPPLIER_TAB t
  WHERE (Inventory_part_API.Get_Part_Status('GLO1X', t.part_no) = 'B')
    AND (Inventory_Part_API.Get_Accounting_Group('GLO1X', t.part_no) != 'ASSET')
    AND vendor_no NOT IN (select contract from SITE_TAB)
    AND (Inventory_Part_API.Part_Exist(t.contract, t.part_no) = 1)
    AND status_code = 2;

   info_                  VARCHAR2(2000);
   objid_                 VARCHAR2(2000);
   objversion_            VARCHAR2(2000);
   attr_                  VARCHAR2(2000);
   key_values_            VARCHAR2(1000);
BEGIN
  FOR pur_part_supplier_ IN get_pur_part_supplier LOOP
      client_sys.Clear_Attr(attr_);
      client_sys.Add_To_Attr('PRIMARY_VENDOR_DB','N', attr_);
      client_sys.Add_To_Attr('STATUS_CODE',1, attr_);
      objid_      := pur_part_supplier_.rowid;
      objversion_ := pur_part_supplier_.objvers;
      BEGIN
         Purchase_Part_Supplier_API.Modify__(info_,objid_,objversion_,attr_,'DO');
      EXCEPTION
         WHEN OTHERS THEN
            key_values_ := 'PURCHASE_PART: '||pur_part_supplier_.part_no||' CONTRACT: '||pur_part_supplier_.contract;
            Log_Error ('MT9_Upd_Purch_Part_Supp_Stat', SQLCODE, SQLERRM, key_values_);
      END;
   END LOOP;
END MT9_Upd_Purch_Part_Supp_Stat;

------------------------------------------------------------------------------
-- Internal Purchase Part Supplier records need to have a consisten UoM across
-- the system, between sites and also in comparison to the Inventory Uom. When
-- a PPS record doesn't have the correct UoM, this procedure corrects it
--
PROCEDURE Mt10_Upd_Purch_Part_Supp_Uom
IS
   info_   VARCHAR2(2000);
   attr_   VARCHAR2(2000);
   keys_   VARCHAR2(1000);
   nr_     NUMBER  := 0;
   nrf_    NUMBER  := 0;
   CURSOR get_internal_diff_sales_uom IS
      SELECT pps.part_no, pps.contract, pps.vendor_no,
             ip.unit_meas ip_uom, pps.buy_unit_meas, pps.price_unit_meas,
             pps.objid, pps.objversion
      FROM   purchase_part_supplier pps
             INNER JOIN inventory_part ip
                     ON ip.contract   = pps.contract
                    AND ip.part_no    = pps.part_no
                    AND ip.unit_meas != pps.buy_unit_meas
      WHERE  pps.vendor_no IN (SELECT contract FROM site_tab);

BEGIN
   FOR pps_ IN get_internal_diff_sales_uom LOOP
      BEGIN
         Client_SYS.Clear_Attr  (attr_);
         Client_SYS.Add_To_Attr ('BUY_UNIT_MEAS',     pps_.ip_uom, attr_);
         Client_SYS.Add_To_Attr ('CONV_FACTOR',       1,           attr_);
         Client_SYS.Add_To_Attr ('PRICE_CONV_FACTOR', 1,           attr_);
         Purchase_Part_Supplier_API.Modify__ (info_, pps_.objid, pps_.objversion, attr_, 'DO');
         nr_ := nr_ + 1;
      EXCEPTION
         WHEN OTHERS THEN
            keys_ := 'PURCHASE_PART: ' || pps_.part_no || ' CONTRACT: ' || pps_.contract;
            Log_Error ('MT10_Upd_Purch_Part_Supp_UOM', SQLCODE, SQLERRM, keys_);
            nrf_ := nrf_ + 1;
      END;
   END LOOP;
   Cbh_Utils_API.Log_Info ('Mt10_Upd_Purch_Part_Supp_Uom => Records updated: :P1, Records failed: :P2', to_char(nr_), to_char(nrf_));
END Mt10_Upd_Purch_Part_Supp_Uom;

-- Assumption: When calculating the quantity in transit; only part no and contract were considered
PROCEDURE  MT11_Upd_Inv_Part_Zero_Cost_1
IS
   CURSOR get_inv_parts IS
   SELECT part_no, contract, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_tab i
    WHERE accounting_group != 'ASSET'
      AND zero_cost_flag = 'Y'
      AND Inventory_Part_In_Stock_API.Get_Inventory_Qty_Onhand(CONTRACT,PART_NO,NULL) = 0
      AND Inventory_Part_In_Transit_API.Get_Total_Qty_In_Order_Transit(CONTRACT,PART_NO) = 0;

   --CURSOR get_qty_in_transit(part_no_ IN VARCHAR2, contract_ IN VARCHAR2) IS
   --SELECT SUM(quantity)
     --FROM inventory_part_in_transit_tab
    --WHERE part_no = part_no_
      --AND contract = contract_;

   info_                  VARCHAR2(2000);
   objid_                 VARCHAR2(2000);
   objversion_            VARCHAR2(2000);
   attr_                  VARCHAR2(2000);
   key_values_            VARCHAR2(1000);

BEGIN
  FOR inv_part_ IN get_inv_parts LOOP
      --OPEN get_qty_in_transit (inv_part_.part_no, inv_part_.contract);
      --FETCH get_qty_in_transit INTO quantity_temp_;
      --CLOSE get_qty_in_transit;

      --IF (quantity_temp_ = 0 OR quantity_temp_ IS NULL) THEN
        client_sys.Clear_Attr(attr_);
        client_sys.Add_To_Attr('ZERO_COST_FLAG','Zero Cost Forbidden', attr_);
        objid_      := inv_part_.rowid;
        objversion_ := inv_part_.objvers;
        BEGIN
           Inventory_Part_API.Modify__(info_,objid_,objversion_,attr_,'DO');
        EXCEPTION
           WHEN OTHERS THEN
              key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
              Log_Error ('MT11_Upd_Inv_Part_Zero_Cost_1', SQLCODE, SQLERRM, key_values_);
        END;
      --END IF;
      --quantity_temp_ := NULL;
   END LOOP;
END MT11_Upd_Inv_Part_Zero_Cost_1;

PROCEDURE  MT11_Upd_Inv_Part_Zero_Cost_2
IS
  CURSOR get_inv_parts IS
   SELECT part_no, contract, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_tab i
    WHERE accounting_group != 'ASSET'
      AND zero_cost_flag = 'Y'
      --AND inventory_valuation_method != 0
      AND (Inventory_Part_Unit_Cost_API.Get_Inventory_Value_By_Method(contract,
                                                                     part_no,
                                                                     NULL,
                                                                     NULL,
                                                                     NULL) != 0);

   info_                  VARCHAR2(2000);
   objid_                 VARCHAR2(2000);
   objversion_            VARCHAR2(2000);
   attr_                  VARCHAR2(2000);
   key_values_            VARCHAR2(1000);
BEGIN
  FOR inv_part_ IN get_inv_parts LOOP
    client_sys.Clear_Attr(attr_);
    client_sys.Add_To_Attr('ZERO_COST_FLAG','Zero Cost Forbidden', attr_);
    objid_      := inv_part_.rowid;
    objversion_ := inv_part_.objvers;
    BEGIN
       Inventory_Part_API.Modify__(info_,objid_,objversion_,attr_,'DO');
    EXCEPTION
       WHEN OTHERS THEN
          key_values_ := 'INVENTORY_PART: '||inv_part_.part_no||' CONTRACT: '||inv_part_.contract;
          Log_Error ('MT11_Upd_Inv_Part_Zero_Cost_2', SQLCODE, SQLERRM, key_values_);
    END;
   END LOOP;
END MT11_Upd_Inv_Part_Zero_Cost_2;


------------------------------------------------------------------------------
-- Inventory Parts on each site need to have the same tariff code (in IFS, the
-- tarif code is called customs_stat_no).  Whenever an inventory part's tariff
-- code isn't match that of the site GLO1X, it needs to be corrected
--
PROCEDURE  Mt12_Upd_Inv_Part_Tariff_Code
IS
   info_   VARCHAR2(2000);
   attr_   VARCHAR2(2000);
   keys_   VARCHAR2(1000);
   nr_     NUMBER  := 0;
   nrf_    NUMBER  := 0;

   CURSOR get_misaligned_tarif_parts IS
      WITH glo1x_inv_part AS (
         SELECT g.contract, g.part_no, g.customs_stat_no, g.intrastat_conv_factor
         FROM   inventory_part g
         WHERE  g.contract = 'GLO1X'
      ), nonglo_nonasset_active_parts AS (
         SELECT r.*
         FROM   inventory_part r
         WHERE  r.contract         != 'GLO1X'
           AND  r.accounting_group != 'ASSET'
           AND  r.part_status      IN ('A', 'B')
      )
      SELECT ip.contract, ip.part_no, ip.accounting_group, ip.part_status,
             glo.customs_stat_no glo_cstat, nvl(ip.customs_stat_no,'X') ip_cstat,
             glo.intrastat_conv_factor glo_cfactor, ip.objid, ip.objversion
      FROM   nonglo_nonasset_active_parts ip
             INNER JOIN glo1x_inv_part glo
                     ON glo.part_no          = ip.part_no
                    AND glo.customs_stat_no != nvl(ip.customs_stat_no,'X');

BEGIN
   FOR ip_ IN get_misaligned_tarif_parts LOOP
      BEGIN
         Client_SYS.Clear_Attr  (attr_);
         Client_SYS.Add_To_Attr ('CUSTOMS_STAT_NO',       ip_.glo_cstat,   attr_);
         IF ip_.glo_cfactor IS NOT null THEN
            Client_SYS.Add_To_Attr ('INTRASTAT_CONV_FACTOR', ip_.glo_cfactor, attr_);
         END IF;
         Inventory_Part_API.Modify__ (info_, ip_.objid, ip_.objversion, attr_, 'DO');
         nr_ := nr_ + 1;
      EXCEPTION
         WHEN OTHERS THEN
            keys_ := 'INVENTORY_PART: ' || ip_.part_no || ' CONTRACT: ' || ip_.contract;
            Log_Error ('MT12_Upd_Inv_Part_Tariff_Code', SQLCODE, SQLERRM, keys_);
            nrf_ := nrf_ + 1;
      END;
   END LOOP;
   Cbh_Utils_API.Log_Info ('Mt12_Upd_Inv_Part_Tariff_Code => Records updated: :P1, Records failed: :P2', to_char(nr_), to_char(nrf_));
END Mt12_Upd_Inv_Part_Tariff_Code;

PROCEDURE  MT13_Upd_Purch_Line_Price
IS
   CURSOR get_pur_ord_lines IS
   SELECT order_no, line_no, release_no, part_no, buy_unit_price, fbuy_unit_price, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM Purchase_Order_Line_Tab pol
    WHERE contract IN (select contract from SITE_TAB)
      AND (pol.buy_unit_price != 0
      OR pol.fbuy_unit_price != 0)
      AND (Inventory_Part_API.Get_Accounting_Group(pol.contract, pol.part_no) = 'ASSET');

      info_                  VARCHAR2(2000);
      objid_                 VARCHAR2(2000);
      objversion_            VARCHAR2(2000);
      attr_                  VARCHAR2(2000);
      key_values_            VARCHAR2(1000);
BEGIN
   FOR po_line_ IN get_pur_ord_lines LOOP
      client_sys.Clear_Attr(attr_);
      client_sys.Add_To_Attr('BUY_UNIT_PRICE',0, attr_);
      client_sys.Add_To_Attr('FBUY_UNIT_PRICE',0, attr_);
      objid_      := po_line_.rowid;
      objversion_ := po_line_.objvers;
      BEGIN
         Purchase_Order_Line_Api.Modify__(info_,objid_,objversion_,attr_,'DO');
      EXCEPTION
         WHEN OTHERS THEN
            key_values_ := 'ORDER_NO: '||po_line_.order_no||' LINE_NO: '||po_line_.line_no||' RELEASE_NO: '||po_line_.release_no||' PURCH_PART_NO: '||po_line_.part_no;
            Log_Error ('MT13_Upd_Purch_Line_Price', SQLCODE, SQLERRM, key_values_);
      END;
   END LOOP;
END MT13_Upd_Purch_Line_Price;

PROCEDURE  MT14_Upd_Purch_Line_Recpt_Date
IS
   CURSOR get_pol IS
   SELECT POLP.OBJID,
          POLP.OBJVERSION,
          POLP.ORDER_NO,
          POLP.LINE_NO,
          POLP.RELEASE_NO,
          POLP.PART_NO,
          to_char(COJ.REAL_SHIP_DATE + nvl(SITE_TO_SITE_LEADTIME_API.Get_Delivery_Leadtime(COJ.CUSTOMER_NO, COJ.CONTRACT, COJ.SHIP_VIA_CODE), 0) + nvl(SITE_TO_SITE_LEADTIME_API.Get_Internal_Delivery_Leadtime(COJ.CUSTOMER_NO, COJ.CONTRACT, COJ.SHIP_VIA_CODE), 0), 'YYYY-MM-DD') ETA
     FROM PURCHASE_ORDER_LINE_PART POLP
          INNER JOIN PURCHASE_ORDER_LINE_NEW POLN
                ON (
                    POLP.ORDER_NO = POLN.ORDER_NO
                    AND POLP.LINE_NO = POLN.LINE_NO
                    AND POLP.RELEASE_NO = POLN.RELEASE_NO
                )
                INNER JOIN CUSTOMER_ORDER_JOIN COJ
                ON (
                    COJ.DEMAND_ORDER_REF1 = POLN.ORDER_NO
                    AND COJ.DEMAND_ORDER_REF2 = POLN.LINE_NO
                    AND COJ.DEMAND_ORDER_REF3 = POLN.RELEASE_NO
                    AND COJ.REAL_SHIP_DATE IS NOT NULL
                    AND INVENTORY_PART_API.Get_Accounting_Group('GLO1X', POLN.PART_NO) != 'ASSET'
                )
            AND SYSDATE > ANY (POLP.PLANNED_DELIVERY_DATE, POLP.PLANNED_RECEIPT_DATE)
            AND COJ.REAL_SHIP_DATE + nvl(SITE_TO_SITE_LEADTIME_API.Get_Delivery_Leadtime(COJ.CUSTOMER_NO, COJ.CONTRACT, COJ.SHIP_VIA_CODE), 0) + nvl(SITE_TO_SITE_LEADTIME_API.Get_Internal_Delivery_Leadtime(COJ.CUSTOMER_NO, COJ.CONTRACT, COJ.SHIP_VIA_CODE), 0) > POLP.PLANNED_DELIVERY_DATE;

    info_       VARCHAR2(2000);
    attr_       VARCHAR2(2000);
    key_values_ VARCHAR2(1000);

BEGIN
  FOR pol_ IN get_pol LOOP
        Client_SYS.Clear_Attr(attr_);
        Client_SYS.Add_To_Attr('PLANNED_DELIVERY_DATE', pol_.ETA, attr_);
        Client_SYS.Add_To_Attr('PLANNED_RECEIPT_DATE', pol_.ETA, attr_);

     BEGIN
         PURCHASE_ORDER_LINE_PART_API.Modify__(info_, pol_.objid, pol_.objversion, attr_, 'DO');
     EXCEPTION
         WHEN OTHERS THEN
            key_values_ := 'ORDER_NO: '||pol_.order_no||' LINE_NO: '||pol_.line_no||' RELEASE_NO: '||pol_.release_no||' PURCH_PART_NO: '||pol_.part_no;
            Log_Error ('MT14_Upd_Purch_Line_Recpt_Date', SQLCODE, SQLERRM, key_values_);
     END;
   END LOOP;
END MT14_Upd_Purch_Line_Recpt_Date;

PROCEDURE  MT15_Upd_Purch_Part_Supp_Curr
IS
  CURSOR get_purch_part_supplier IS
   SELECT part_no, contract, vendor_no, currency_code, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
    FROM Purchase_Part_Supplier_TAB
   WHERE vendor_no IN (select contract from SITE_TAB);


   info_                  VARCHAR2(2000);
   objid_                 VARCHAR2(2000);
   objversion_            VARCHAR2(2000);
   attr_                  VARCHAR2(2000);

   selected_currency_ VARCHAR2(5);
   company_currency_  VARCHAR2(5);
   key_values_        VARCHAR2(1000);
BEGIN
  FOR supp_for_pp_ IN get_purch_part_supplier LOOP
    selected_currency_ := supp_for_pp_.currency_code;
    company_currency_  := COMPANY_FINANCE_API.Get_Currency_Code(SITE_API.Get_Company(lpad(supp_for_pp_.vendor_no, 5)));
    IF ((selected_currency_ IS NOT NULL) AND (company_currency_ IS NOT NULL) AND (selected_currency_ != company_currency_))THEN
      client_sys.Clear_Attr(attr_);
      client_sys.Add_To_Attr('CURRENCY_CODE',company_currency_, attr_);
      objid_      := supp_for_pp_.rowid;
      objversion_ := supp_for_pp_.objvers;
      BEGIN
         PURCHASE_PART_SUPPLIER_API.Modify__(info_,objid_,objversion_,attr_,'DO');
      EXCEPTION
         WHEN OTHERS THEN
            key_values_ := 'PART_NO: '||supp_for_pp_.part_no||' CONTRACT: '||supp_for_pp_.contract||' VENDOR_NO: '||supp_for_pp_.vendor_no;
            Log_Error ('MT15_Upd_Purch_Part_Supp_Curr', SQLCODE, SQLERRM, key_values_);
      END;
    END IF;
  END LOOP;
END MT15_Upd_Purch_Part_Supp_Curr;

PROCEDURE  MT16_Upd_Purch_Part_Supp_Price
IS
  CURSOR get_purch_part_supplier IS
  SELECT part_no, contract, vendor_no, currency_code, list_price, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
    FROM Purchase_Part_Supplier_TAB
   WHERE list_price != 0
     AND Inventory_part_API.Get_Accounting_Group(contract, part_no) = 'ASSET';

  info_                  VARCHAR2(2000);
   objid_                 VARCHAR2(2000);
   objversion_            VARCHAR2(2000);
   attr_                  VARCHAR2(2000);
   key_values_            VARCHAR2(1000);
BEGIN
  FOR supp_for_pp_ IN get_purch_part_supplier LOOP
    IF (supp_for_pp_.list_price != 0)THEN
      client_sys.Clear_Attr(attr_);
      client_sys.Add_To_Attr('LIST_PRICE',0, attr_);
      objid_      := supp_for_pp_.rowid;
      objversion_ := supp_for_pp_.objvers;
      BEGIN
         PURCHASE_PART_SUPPLIER_API.Modify__(info_,objid_,objversion_,attr_,'DO');
      EXCEPTION
         WHEN OTHERS THEN
            key_values_ := 'PART_NO: '||supp_for_pp_.part_no||' CONTRACT: '||supp_for_pp_.contract||' VENDOR_NO: '||supp_for_pp_.vendor_no;
            Log_Error ('MT16_Upd_Purch_Part_Supp_Price', SQLCODE, SQLERRM, key_values_);
      END;
    END IF;
  END LOOP;
END MT16_Upd_Purch_Part_Supp_Price;

PROCEDURE Mt17_Upd_Purch_Part_Supp_Part (
   limit_    IN NUMBER,
   rollback_ IN VARCHAR2 DEFAULT 'NO' )
IS

   args_      VARCHAR2(2000);
   total_     NUMBER;
   nr_        NUMBER       := 0;
   job_start_ DATE         := sysdate;
   procedure_ VARCHAR2(60) := 'Ta_Maintenance_Util_API.Mt17_Upd_Pps_Vendor_Part_Bkg';
   job_desc_  VARCHAR2(71) := 'TA Maintenance: Vendor Part No update on PPS';

   CURSOR get_spurious_intersite_pps IS
      SELECT count(1) over() nr, pps.contract, pps.part_no, pps.vendor_no,
             pps.vendor_part_no, pps.objid, pps.objversion,
             ip.part_status ip_status, ipv.part_status vendor_status
      FROM   purchase_part_supplier pps
             INNER JOIN inventory_part ip
                     ON ip.contract    = pps.contract
                    AND ip.part_no     = pps.part_no
                    AND ip.part_status IN ('A', 'B', 'G')
             INNER JOIN inventory_part ipv
                     ON ipv.contract    = pps.vendor_no
                    AND ipv.part_no     = pps.part_no
                    AND ipv.part_status IN ('A', 'B', 'G')
             INNER JOIN sales_part sp
                     ON sp.contract   = pps.contract
                    AND sp.catalog_no = pps.part_no
      WHERE  pps.vendor_no IN (SELECT s.contract FROM site_tab s)
        AND  nvl (pps.vendor_part_no, 'xx') != pps.part_no;

BEGIN
   FOR pps_ IN get_spurious_intersite_pps LOOP
      EXIT WHEN limit_ != -1 AND nr_ >= limit_;
      total_ := CASE
         WHEN limit_ = -1 THEN pps_.nr
         ELSE least (pps_.nr, limit_)
      END;
      nr_ := nr_ + 1;
      Client_SYS.Clear_Attr  (args_);
      Client_SYS.Add_To_Attr ('OBJID_',      pps_.objid,      args_);
      Client_SYS.Add_To_Attr ('OBJVER_',     pps_.objversion, args_);
      Client_SYS.Add_To_Attr ('CONTRACT_',   pps_.contract,   args_);
      Client_SYS.Add_To_Attr ('PART_NO_',    pps_.part_no,    args_);
      Client_SYS.Add_To_Attr ('VENDOR_',     pps_.vendor_no,  args_);
      Client_SYS.Add_To_Attr ('START_TIME_', job_start_,      args_);
      Client_SYS.Add_To_Attr ('JOB_NO_',     nr_,             args_);
      Client_SYS.Add_To_Attr ('JOB_TOTAL_',  total_,          args_);
      Client_SYS.Add_To_Attr ('ROLLBACK_',   rollback_,       args_);
      Transaction_SYS.Deferred_Call (procedure_, 'PARAMETER', args_, job_desc_, sysdate, 'TRUE');
   END LOOP;
   Cbh_Utils_API.Log_Progress ('Launched :P1 background jobs to update PPS Vendor Part Number', to_char(nr_));
END Mt17_Upd_Purch_Part_Supp_Part;

PROCEDURE Mt17_Upd_Pps_Vendor_Part_Bkg (
   objid_      IN VARCHAR2,
   objver_     IN VARCHAR2,
   contract_   IN VARCHAR2,
   part_no_    IN VARCHAR2,
   vendor_     IN VARCHAR2,
   start_time_ IN DATE     DEFAULT null,
   job_no_     IN NUMBER   DEFAULT null,
   job_total_  IN NUMBER   DEFAULT null,
   rollback_   IN VARCHAR2 DEFAULT 'NO' )
IS
   info_      VARCHAR2(2000);
   attr_      VARCHAR2(2000);
   objvers_   VARCHAR2(2000) := objver_;
BEGIN
   Client_SYS.Clear_Attr  (attr_);
   Client_SYS.Add_To_Attr ('VENDOR_PART_NO', part_no_, attr_);
   --Case 0001 added comment
   Purchase_Part_Supplier_API.Modify__ (info_, objid_, objvers_, attr_, 'DO');
   Cbh_Utils_API.Log_Info ('Updated PPS record :P1 / :P2 / :P3', part_no_, contract_, vendor_);
   IF rollback_ = 'YES' THEN
      ROLLBACK;
      Cbh_Utils_API.Log_Info ('Transaction rolled back');
   END IF;
   Inform_User ('PPS Vendor Part No', start_time_, job_no_, job_total_);
END Mt17_Upd_Pps_Vendor_Part_Bkg;

PROCEDURE  Mt18_Upd_Sales_Part_Sales_Grp
IS
   info_        VARCHAR2(2000);
   attr_        VARCHAR2(2000);
   key_values_  VARCHAR2(1000);
   acc_grp_     VARCHAR2(5);

   CURSOR get_sales_parts IS
      SELECT sp.contract, sp.part_no, sp.catalog_group, sp.objid, sp.objversion objver
      FROM   sales_part sp
      WHERE  sp.catalog_type = 'INV'
        AND  Inventory_Part_API.Get_Part_Status (sp.contract, sp.part_no) IN ('A', 'B');

BEGIN
   FOR sp_ IN get_sales_parts LOOP
      acc_grp_ := Inventory_part_API.Get_Accounting_Group(sp_.contract,sp_.part_no);
      IF sp_.catalog_group != acc_grp_ THEN
         BEGIN
            Client_SYS.Clear_Attr  (attr_);
            Client_SYS.Add_To_Attr ('CATALOG_GROUP', acc_grp_, attr_);
            Sales_Part_API.Modify__ (info_, sp_.objid, sp_.objver, attr_, 'DO');
         EXCEPTION
            WHEN OTHERS THEN
               key_values_ := 'PART_NO: '||sp_.part_no||' CONTRACT: '||sp_.contract||' CATALOG_GROUP: '||sp_.catalog_group;
               Log_Error ('MT18_Upd_Sales_Part_Sales_Grp', SQLCODE, SQLERRM, key_values_);
         END;
      END IF;
   END LOOP;
END Mt18_Upd_Sales_Part_Sales_Grp;


PROCEDURE Mt19_Upd_Pps_Interco_Price (
   limit_    IN NUMBER,
   part_no_  IN VARCHAR2 DEFAULT '%',
   rollback_ IN VARCHAR2 DEFAULT 'NO' )
IS

   args_      VARCHAR2(2000);
   nr_        NUMBER       := 0;
   job_start_ DATE         := sysdate;
   procedure_ VARCHAR2(60) := 'Ta_Maintenance_Util_API.Mt19_Upd_Pps_Price_Bkg';
   job_desc_  VARCHAR2(71) := 'TA Maintenance: List Price update on PPS record';

   CURSOR get_spurious_list_prices IS
      WITH list_v_inv_price AS (
         SELECT pps.part_no, pps.contract, pps.vendor_no,
                Company_Finance_API.Get_Currency_Code (Site_API.Get_Company(pps.contract)) buyer_curr,
                pps.list_price vendor_price_current, pps.currency_code vendor_curr,
                nvl (
                   Inventory_Part_Unit_Cost_API.Get_Inventory_Value_By_Method (
                      pps.vendor_no, pps.part_no, null, null, null ),
                   -999
                ) inventory_val_vendor,
                pps.objid, pps.objversion objver
         FROM   purchase_part_supplier pps
      )
      SELECT v.part_no, v.contract, v.vendor_no, v.buyer_curr,
             v.vendor_price_current, v.vendor_curr,
             v.inventory_val_vendor, v.objid, v.objver
      FROM   list_v_inv_price v
      WHERE  v.vendor_no IN (SELECT contract FROM site_tab)
        AND  v.part_no     LIKE part_no_
        AND  v.vendor_price_current != v.inventory_val_vendor
        AND  rownum <= limit_;

BEGIN

   IF limit_ NOT BETWEEN 1 AND 10000 THEN
      Error_SYS.Record_General (lu_name_, 'LIMITCONSTRAINT: Limit must be between 1 and 10,0000');
   END IF;

   FOR lp_ IN get_spurious_list_prices LOOP
      nr_ := nr_ + 1;
      Client_SYS.Clear_Attr  (args_);
      Client_SYS.Add_To_Attr ('OBJID_',      lp_.objid,                args_);
      Client_SYS.Add_To_Attr ('OBJVER_',     lp_.objver,               args_);
      Client_SYS.Add_To_Attr ('CONTRACT_',   lp_.contract,             args_);
      Client_SYS.Add_To_Attr ('PART_NO_',    lp_.part_no,              args_);
      Client_SYS.Add_To_Attr ('VENDOR_',     lp_.vendor_no,            args_);
      Client_SYS.Add_To_Attr ('INV_VALUE_',  lp_.inventory_val_vendor, args_);
      Client_SYS.Add_To_Attr ('START_TIME_', job_start_,               args_);
      Client_SYS.Add_To_Attr ('JOB_NO_',     nr_,                      args_);
      Client_SYS.Add_To_Attr ('JOB_TOTAL_',  limit_,                   args_);
      Client_SYS.Add_To_Attr ('ROLLBACK_',   rollback_,                args_);
      Transaction_SYS.Deferred_Call (procedure_, 'PARAMETER', args_, job_desc_, sysdate, 'TRUE');
   END LOOP;
   Cbh_Utils_API.Log_Progress ('Launched :P1 background jobs to update PPS List Price', to_char(nr_));

END Mt19_Upd_Pps_Interco_Price;

PROCEDURE Mt19_Upd_Pps_Price_Bkg (
   objid_      IN VARCHAR2,
   objver_     IN VARCHAR2,
   contract_   IN VARCHAR2,
   part_no_    IN VARCHAR2,
   vendor_     IN VARCHAR2,
   inv_value_  IN NUMBER,
   start_time_ IN DATE     DEFAULT null,
   job_no_     IN NUMBER   DEFAULT null,
   job_total_  IN NUMBER   DEFAULT null,
   rollback_   IN VARCHAR2 DEFAULT 'NO' )
IS
   info_      VARCHAR2(2000);
   attr_      VARCHAR2(2000);
   objvers_   VARCHAR2(2000) := objver_;
BEGIN
   Client_SYS.Clear_Attr  (attr_);
   Client_SYS.Add_To_Attr ('LIST_PRICE', inv_value_, attr_);
   Purchase_Part_Supplier_API.Modify__ (info_, objid_, objvers_, attr_, 'DO');
   Cbh_Utils_API.Log_Info (
      'Updated list price on PPS record :P1 / :P2 / :P3 to :P4',
      part_no_, contract_, vendor_, to_char(inv_value_)
   );
   IF rollback_ = 'YES' THEN
      ROLLBACK;
      Cbh_Utils_API.Log_Info ('Transaction rolled back');
   END IF;
   Inform_User ('PPS List Price', start_time_, job_no_, job_total_);
END Mt19_Upd_Pps_Price_Bkg;


PROCEDURE Mt20_Upd_Sales_Price
IS

   inv_value_   NUMBER;
   info_        VARCHAR2(2000);
   attr_        VARCHAR2(2000);
   key_values_  VARCHAR2(1000);

   CURSOR get_sales_parts IS
      SELECT sp.contract, sp.list_price, sp.part_no, sp.catalog_group,
             sp.objid, sp.objversion objver
      FROM   sales_part sp
      WHERE  Inventory_Part_API.Get_Part_Status (sp.contract, sp.part_no) IN ('A', 'B')
        AND  Inventory_Part_API.Get_Accounting_Group (sp.contract, sp.part_no) != 'ASSET';

BEGIN
   FOR sp_ IN get_sales_parts LOOP
      inv_value_ := Inventory_Part_Unit_Cost_API.Get_Inventory_Value_By_Method (
         sp_.contract, sp_.part_no, null, null, null
      );
      IF sp_.list_price != inv_value_ THEN
         BEGIN
            Client_SYS.Clear_Attr  (attr_);
            Client_SYS.Add_To_Attr ('LIST_PRICE', inv_value_, attr_);
            Sales_Part_API.Modify__(info_, sp_.objid, sp_.objver, attr_, 'DO');
         EXCEPTION
            WHEN OTHERS THEN
               key_values_ := 'PART_NO: '||sp_.part_no||' CONTRACT: '||sp_.contract||' CATALOG_GROUP: '||sp_.catalog_group;
               Log_Error ('MT20_Upd_Sales_Price', SQLCODE, SQLERRM, key_values_);
         END;
      END IF;
   END LOOP;
END Mt20_Upd_Sales_Price;

PROCEDURE  MT21_Upd_Inv_Part_Est_Mat_Cost
IS
   CURSOR get_inv_parts IS
   SELECT contract, part_no
     FROM inventory_part_tab;

   CURSOR get_part_configs(part_no_ IN VARCHAR2, contract_ IN VARCHAR2) IS
   SELECT part_no, contract, configuration_id, estimated_material_cost, rowid, ltrim(lpad(to_char(rowversion,'YYYYMMDDHH24MISS'),2000)) objvers
     FROM inventory_part_config_tab
    WHERE part_no = part_no_
      AND contract = contract_;

   inv_p_sum_inv_val_ NUMBER;

   info_                  VARCHAR2(2000);
   objid_                 VARCHAR2(2000);
   objversion_            VARCHAR2(2000);
   attr_                  VARCHAR2(2000);
   key_values_            VARCHAR2(1000);

BEGIN
  FOR inv_part_ IN get_inv_parts LOOP
    FOR config_ IN get_part_configs(inv_part_.part_no, inv_part_.contract) LOOP
        inv_p_sum_inv_val_ := Inventory_Part_Unit_Cost_API.Get_Inventory_Value_By_Method(config_.contract,
                                                                                         config_.part_no,
                                                                                         NULL,
                                                                                         NULL,
                                                                                         NULL);

        IF ((inv_p_sum_inv_val_ != 0) AND (inv_p_sum_inv_val_ != config_.estimated_material_cost)) THEN
          client_sys.Clear_Attr(attr_);
          client_sys.Add_To_Attr('ESTIMATED_MATERIAL_COST',inv_p_sum_inv_val_, attr_);
          objid_      := config_.rowid;
          objversion_ := config_.objvers;
          BEGIN
             Inventory_Part_Config_Api.Modify__(info_,objid_,objversion_,attr_,'DO');
          EXCEPTION
            WHEN OTHERS THEN
                 key_values_ := 'PART_NO: '||config_.part_no||' CONTRACT: '||config_.contract||' CATALOG_GROUP: '||config_.configuration_id;
                 Log_Error ('MT21_Upd_Inv_Part_Est_Mat_Cost', SQLCODE, SQLERRM, key_values_);
          END;
        END IF;
    END LOOP;
  END LOOP;
END MT21_Upd_Inv_Part_Est_Mat_Cost;

PROCEDURE  Log_Error (
   method_name_    IN VARCHAR2,
   error_code_     IN VARCHAR2,
   error_message_  IN VARCHAR2,
   key_values_     IN VARCHAR2,
   log_time_       IN DATE DEFAULT sysdate )
IS BEGIN
  INSERT INTO ta_maintenance_log_tab VALUES (log_time_, method_name_, error_code_, error_message_, key_values_);
END Log_Error;

PROCEDURE Clear_Error_Log
IS
BEGIN
  DELETE FROM TA_MAINTENANCE_LOG_TAB;
  COMMIT;
END Clear_Error_Log;

END TA_MAINTENANCE_UTIL_API;
/
