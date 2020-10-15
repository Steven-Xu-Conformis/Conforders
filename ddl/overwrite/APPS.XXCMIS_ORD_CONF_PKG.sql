create or replace PACKAGE      XXCMIS_ORD_CONF_PKG
AS
/******************************************************************************
 NAME:         APPS.XXCMIS_ORD_CONF_PKG
 PURPOSE:   To populate the table for orders.conformis to fetch the order data

   REVISIONS:
   Ver               Date                 Author                  Description
   ---------        --------------      ---------------         ------------------------------------
   1.0              14-FEB-2018       Priya H              Initial Version
*******************************************************************************/

   PKB_RESP_ID      NUMBER := fnd_global.RESP_ID;
   PKB_RESP_APPL_ID NUMBER := fnd_global.RESP_APPL_ID;
   PKB_USER_ID      NUMBER := fnd_global.USER_ID;
   PKB_SYSDATE      DATE   := SYSDATE; 
      
PROCEDURE MAIN (RETCODE       OUT NUMBER,
                ERRBUF        OUT VARCHAR2
                                      );                            
                                      
END XXCMIS_ORD_CONF_PKG;


create or replace PACKAGE BODY      XXCMIS_ORD_CONF_PKG
AS
/******************************************************************************
 NAME:         APPS.XXCMIS_ORD_CONF_PKG
 PURPOSE:      Populate the table for orders.conformis to fetch the order data

   REVISIONS:
   Ver               Date                 Author                  Description
   ---------        --------------      ---------------         ----------------
   1.0              14-FEB-2018       Priya H              Initial Version
   2.0              23-Oct-2018       Swapnesh P           HIP status Change
   3.0              08-Feb-2018       Swapnesh P           #22784 Order not showing wrong order status - Orders.Conformis
   4.0              11-Dec-2019       Infosense            Added logic to show IRF ID order status - Orders.Conformis.
   5.0				26-Dec-2019		  Infosense			   Added logic to show PRODUCT DESC ORDCNF, ORDER DETAILS ORDCNF order status - Orders.Conformis.
   6.0              22-Jan-2020       Infosense            Added Logic to remove multiple lines getting for G3/ Identity Orders - Order.Conformis.
   7.0              22-Feb-2020		  Infosense			   Added logic to show ORDER_DETAILS_2_ORDCNF order status - Orders.Conformis.
   8.0              31-Mar-2020       Infosense            Added logic to change existing of 'ACTUAL_SURGERY_DATE' to 'RESCHEDULED_SURGERY_DATE' and added
	                                                       DFF Attribute7 ('ACTUAL_SURGERY_DATE').
*******************************************************************************/

--v2.0
Function get_hip_rev_status( l_serial_number varchar2) return varchar is
 l_status varchar2(100);
begin
  select status into l_status
    from conforders.hip_revision
      where serial_number = l_serial_number
        and revision = (select max(revision)
                          from conforders.hip_revision
                            where serial_number = l_serial_number);
  return upper(l_status);
 exception when others then
  l_status := null;
  return upper(l_status);
 end  get_hip_rev_status;

PROCEDURE MAIN (RETCODE       OUT NUMBER,
                ERRBUF        OUT VARCHAR2) IS

    l_cnt number :=0;
    l_line_status varchar2(200); --v2.0
    l_hip_rev_status varchar2(100); --Added V5.0

	--Add Start V5.0 --Added bulk collection to increase performace.

    TYPE XXMIS_BLUK IS TABLE OF CONFORDERS.XXCMIS_CONF_ORDERS_V%ROWTYPE;
    l_data XXMIS_BLUK;

    TYPE XXMIS_BLUK1 IS TABLE OF APPS.XXCMIS_G3_ORDERS_CONFORMIS_V%ROWTYPE;
    l_data1 XXMIS_BLUK1;

    BatchSize number := 1000;

	--Add End V5.0

    CURSOR C1 is
    select * 	--Added V5.0
    --Start Commented for V5.0
/*          ORDER_ID, SURGEON_ID, SURGEON_TITLE, SURGEON_LAST_NAME, SURGEON_FIRST_NAME, PATIENT_FIRST_NAME, PATIENT_LAST_NAME, PATIENT_GENDER, PATIENT_DOB,
       CATALOG_NUMBER, UNIT_SELLING_PRICE, SERIAL_NUMBER, LINE_ID, PRODUCT_CATEGORY, UDI_CATALOG, PRODUCT_DESCRIPTION, PATELLA_FLAG, SCHED_SURGERY_DATE,
        COMMITMENT_DATE, SURGERY_LOCATION, STATUS, IMPLANT_REQUEST_DATE, SCAN_RECEIVED_DATE, SCAN_CP_FLAG,
       SHIPPED_DATE, REGION_CODE, SALES_REP_NAME, TERRITORY_CODE, COUNTRY_CODE,
       ACTUAL_SURGERY_DATE, CURRENCY, HOSPITAL_NAME, SALESREP_NUMBER, LINE_TYPE
       ,IRF_ID --Added for V4.0
       */
	   --End Commented for V5.0
       from CONFORDERS.XXCMIS_CONF_ORDERS_V d
       where d.line_type not in ('HIP Procedure','HIP Alt Kit to Sales Rep','HIP RMA Alt Kit SRep Rec Only', 'HIP RMA Proc Cust Rec ',
                               'HIP Alt Kit Item to Customer', 'HIP Alt Kit to Sales Rep','HIP Patient Specific') -- Added line type HIP Patient Specific to display only the HIP Design line --07/02/2018
        --where line_type_id not in  (1207, 1208, 1209);
         /*     --Comment Started V6.0
         --Add Start V5.0
        AND not exists (select 1
                        from
                            conforders.irf xxcmis_irf,
                            xxcmis_irf_orders_status_tbl xiost
                        where xiost.irfid = xxcmis_irf.id
                        and xiost.header_id = d.header_id
                        and xxcmis_irf.product = 'iTotal Identity CR')
        --Added End V5.0
        */      --Comment Ended V6.0
        --Add Start V6.0
        AND not exists (select 1
                        from
                            apps.oe_order_lines_all oola,
                            apps.MTL_SYSTEM_ITEMS_B MST,
                            apps.mtl_item_categories micat,
                            apps.mtl_categories mcat,
                            apps.mtl_category_sets mcats,
                            apps.mtl_item_categories micat1,
                            apps.mtl_categories mcat1,
                            apps.mtl_category_sets mcats1
                        WHERE mcats.category_set_name = 'Inventory'
                        AND micat.category_set_id = mcats.category_set_id
                        AND micat.category_id = mcat.category_id
                        AND MST.inventory_item_id = micat.inventory_item_id
                        AND MST.organization_id =  micat.organization_id
                        AND mcat.SEGMENT1 = 'WIP CLASSIFICATION'
                        AND mcat.SEGMENT2 = 'PATIENT SPECIFIC - KNEE IDENTITY'
                        AND mcats1.category_set_name = 'OrdCnf Order Status Include'
                        AND micat1.category_set_id = mcats1.category_set_id
                        AND micat1.category_id = mcat1.category_id
                        AND MST.inventory_item_id = micat1.inventory_item_id
                        AND MST.organization_id =  micat1.organization_id
                        AND oola.inventory_item_id = mst.inventory_item_id
                        AND mcat1.SEGMENT1 = 'Yes'
                        AND oola.flow_status_code NOT IN ('RETURN', 'CANCELLED')
                        AND oola.header_id = d.header_id)
        --Add End V6.0
        ;

       --Add Start V5.0
       CURSOR G3_ORDER_C IS
            select *
           from APPS.XXCMIS_G3_ORDERS_CONFORMIS_V;
       --Add End V5.0

  BEGIN
        FND_FILE.PUT_LINE (FND_FILE.LOG,'=========Calling populate order table proc=============');

        DELETE XXCMIS.XXCMIS_CONF_ORD;
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Deleted table XCMIS_CONF_ORD ');

        --Start Commented for V5.0
          /*FOR x IN C1 LOOP

                      /*   FND_GLOBAL.APPS_INITIALIZE (PKB_USER_ID, PKB_RESP_ID, PKB_RESP_APPL_ID);
                         MO_GLOBAL.INIT ('OM');
                         MO_GLOBAL.SET_POLICY_CONTEXT ('S', 82);*//*

                     --   FND_FILE.PUT_LINE (FND_FILE.Log,'Pass Order Number==>'||HEADER_REC.ORDER_NUMBER);
                      --  FND_FILE.PUT_LINE (FND_FILE.Log,'Org Id==>'||HEADER_REC.ORG_ID);

             --------v2.0 Start-----
          l_line_status := NULL; -- v3.0
         if x.status = 'PENDING_DESIGN_APPROVAL' then
               if  get_hip_rev_status(x.SERIAL_NUMBER) = 'PENDING' then
                 l_line_status := 'Pending Surgeon Review';--'Pending Surgeon Execution';
               elsif get_hip_rev_status(x.SERIAL_NUMBER) = 'DECLINED' then
                 l_line_status := 'Re-Design';
               elsif get_hip_rev_status(x.SERIAL_NUMBER) = 'APPROVED' then
                 l_line_status := 'Design Approved by Surgeon';
               else
                  l_line_status := x.status; --v3.0 added
               end if;
             else
                l_line_status := x.status;
             end if;
         -------V2.0 End--------

               INSERT INTO XXCMIS.XXCMIS_CONF_ORD
                                                 (ORDER_ID,
                                                 SURGEON_ID,
                                                 SURGEON_TITLE,
                                                 SURGEON_LAST_NAME,
                                                 SURGEON_FIRST_NAME,
                                                 PATIENT_FIRST_NAME,
                                                 PATIENT_LAST_NAME,
                                                 PATIENT_GENDER,
                                                 PATIENT_DOB,
                                                 CATALOG_NUMBER,
                                                 UNIT_SELLING_PRICE,
                                                 SERIAL_NUMBER,
                                                 LINE_ID,
                                                 PRODUCT_CATEGORY,
                                                 UDI_CATALOG,
                                                 PRODUCT_DESCRIPTION,
                                                 PATELLA_FLAG,
                                                 SCHED_SURGERY_DATE,
                                                 COMMITMENT_DATE,
                                                 SURGERY_LOCATION,
                                                 STATUS,
                                                 IMPLANT_REQUEST_DATE,
                                                 SCAN_RECEIVED_DATE,
                                                 SCAN_CP_FLAG,
                                                 SHIPPED_DATE,
                                                 REGION_CODE,
                                                 SALES_REP_NAME,
                                                 TERRITORY_CODE,
                                                 COUNTRY_CODE,
                                                 ACTUAL_SURGERY_DATE,
                                                 CURRENCY,
                                                 HOSPITAL_NAME,
                                                 SALESREP_NUMBER,
                                                 LINE_TYPE
                                                 ,IRF_ID --Added V4.0
                                                 )
                                          VALUES(x.ORDER_ID,
                                                 x.SURGEON_ID,
                                                 x.SURGEON_TITLE,
                                                 x.SURGEON_LAST_NAME,
                                                 x.SURGEON_FIRST_NAME,
                                                 x.PATIENT_FIRST_NAME,
                                                 x.PATIENT_LAST_NAME,
                                                 x.PATIENT_GENDER,
                                                 x.PATIENT_DOB,
                                                 x.CATALOG_NUMBER,
                                                 x.UNIT_SELLING_PRICE,
                                                 x.SERIAL_NUMBER,
                                                 x.LINE_ID,
                                                 x.PRODUCT_CATEGORY,
                                                 x.UDI_CATALOG,
                                                 x.PRODUCT_DESCRIPTION,
                                                 x.PATELLA_FLAG,
                                                 x.SCHED_SURGERY_DATE,
                                                 x.COMMITMENT_DATE,
                                                 x.SURGERY_LOCATION,
                                                 l_line_status,     --v2.0 Changed from "x.STATUS"
                                                 x.IMPLANT_REQUEST_DATE,
                                                 x.SCAN_RECEIVED_DATE,
                                                 x.SCAN_CP_FLAG,
                                                 x.SHIPPED_DATE,
                                                 x.REGION_CODE,
                                                 x.SALES_REP_NAME,
                                                 x.TERRITORY_CODE,
                                                 x.COUNTRY_CODE,
                                                 x.ACTUAL_SURGERY_DATE,
                                                 x.CURRENCY,
                                                 x.HOSPITAL_NAME,
                                                 x.SALESREP_NUMBER,
                                                 x.LINE_TYPE
                                                 ,x.IRF_ID --Added V4.0
                                                 );
                   l_cnt := l_cnt + 1;
        end loop;*/
        --End Commented for V5.0

       --Added Start V5.0
       fnd_file.put_line(fnd_file.log, 'Before Non G3 Record Insert. ' || current_timestamp);
      OPEN c1;

        LOOP
            FETCH c1 BULK COLLECT INTO l_data LIMIT batchsize;
            FOR j IN 1..l_data.count LOOP
                IF l_data(j).status = 'PENDING_DESIGN_APPROVAL' THEN
                    l_hip_rev_status := get_hip_rev_status(l_data(j).serial_number);
                    IF l_hip_rev_status = 'PENDING' THEN
                        l_data(j).status := 'Pending Surgeon Review';
                    ELSIF l_hip_rev_status = 'DECLINED' THEN
                        l_data(j).status := 'Re-Design';
                    ELSIF l_hip_rev_status = 'APPROVED' THEN
                        l_data(j).status := 'Design Approved by Surgeon';
                    END IF;
                END IF;
            END LOOP;

        FORALL x IN l_data.first .. l_data.last
        INSERT INTO XXCMIS.XXCMIS_CONF_ORD(ORDER_ID,
                                     SURGEON_ID,
                                     SURGEON_TITLE,
                                     SURGEON_LAST_NAME,
                                     SURGEON_FIRST_NAME,
                                     PATIENT_FIRST_NAME,
                                     PATIENT_LAST_NAME,
                                     PATIENT_GENDER,
                                     PATIENT_DOB,
                                     CATALOG_NUMBER,
                                     UNIT_SELLING_PRICE,
                                     SERIAL_NUMBER,
                                     LINE_ID,
                                     PRODUCT_CATEGORY,
                                     UDI_CATALOG,
                                     PRODUCT_DESCRIPTION,
                                     PATELLA_FLAG,
                                     SCHED_SURGERY_DATE,
                                     COMMITMENT_DATE,
                                     SURGERY_LOCATION,
                                     STATUS,
                                     IMPLANT_REQUEST_DATE,
                                     SCAN_RECEIVED_DATE,
                                     SCAN_CP_FLAG,
                                     SHIPPED_DATE,
                                     REGION_CODE,
                                     SALES_REP_NAME,
                                     TERRITORY_CODE,
                                     COUNTRY_CODE,
                                     --ACTUAL_SURGERY_DATE, --Commented by 8.0
                                     RESCHEDULED_SURGERY_DATE, --Added by 8.0
                                     CURRENCY,
                                     HOSPITAL_NAME,
                                     SALESREP_NUMBER,
                                     LINE_TYPE
                                     ,IRF_ID
                                     ,ACTUAL_SURGERY_DATE
                                     )
              VALUES (l_data(x).ORDER_ID,
                                     l_data(x).SURGEON_ID,
                                     l_data(x).SURGEON_TITLE,
                                     l_data(x).SURGEON_LAST_NAME,
                                     l_data(x).SURGEON_FIRST_NAME,
                                     l_data(x).PATIENT_FIRST_NAME,
                                     l_data(x).PATIENT_LAST_NAME,
                                     l_data(x).PATIENT_GENDER,
                                     l_data(x).PATIENT_DOB,
                                     l_data(x).CATALOG_NUMBER,
                                     l_data(x).UNIT_SELLING_PRICE,
                                     l_data(x).SERIAL_NUMBER,
                                     l_data(x).LINE_ID,
                                     l_data(x).PRODUCT_CATEGORY,
                                     l_data(x).UDI_CATALOG,
                                     l_data(x).PRODUCT_DESCRIPTION,
                                     l_data(x).PATELLA_FLAG,
                                     l_data(x).SCHED_SURGERY_DATE,
                                     l_data(x).COMMITMENT_DATE,
                                     l_data(x).SURGERY_LOCATION,
                                     l_data(x).status,
                                     l_data(x).IMPLANT_REQUEST_DATE,
                                     l_data(x).SCAN_RECEIVED_DATE,
                                     l_data(x).SCAN_CP_FLAG,
                                     l_data(x).SHIPPED_DATE,
                                     l_data(x).REGION_CODE,
                                     l_data(x).SALES_REP_NAME,
                                     l_data(x).TERRITORY_CODE,
                                     l_data(x).COUNTRY_CODE,
                                     --l_data(x).ACTUAL_SURGERY_DATE, --Commented by 8.0
                                     l_data(x).RESCHEDULED_SURGERY_DATE, --Added by 8.0
                                     l_data(x).CURRENCY,
                                     l_data(x).HOSPITAL_NAME,
                                     l_data(x).SALESREP_NUMBER,
                                     l_data(x).LINE_TYPE,
                                     l_data(x).IRF_ID
                                     ,l_data(x).ACTUAL_SURGERY_DATE --Commented by 8.0
                                     );

               l_cnt := l_cnt + 1;
    exit when C1%notfound;
    end loop;
    fnd_file.put_line(fnd_file.log, 'After Non G3 Record Insert. ' || current_timestamp);
    FND_FILE.PUT_LINE (FND_FILE.LOG,'Total Number of Non G3 Records inserted : '||l_cnt);

         open G3_ORDER_C;
          loop
          FETCH G3_ORDER_C BULK COLLECT INTO l_data1 LIMIT BatchSize;

            FOR j1 IN 1..l_data1.count LOOP
                IF l_data1(j1).status = 'PENDING_DESIGN_APPROVAL' THEN
                    l_hip_rev_status := get_hip_rev_status(l_data1(j1).serial_number);
                    IF l_hip_rev_status = 'PENDING' THEN
                        l_data1(j1).status := 'Pending Surgeon Review';
                    ELSIF l_hip_rev_status = 'DECLINED' THEN
                        l_data1(j1).status := 'Re-Design';
                    ELSIF l_hip_rev_status = 'APPROVED' THEN
                        l_data1(j1).status := 'Design Approved by Surgeon';
                    END IF;
                END IF;
            END LOOP;

              FORALL x1 IN l_data1.first .. l_data1.last
              INSERT INTO XXCMIS.XXCMIS_CONF_ORD
                                    (ORDER_ID,
                                     SURGEON_ID,
                                     SURGEON_TITLE,
                                     SURGEON_LAST_NAME,
                                     SURGEON_FIRST_NAME,
                                     PATIENT_FIRST_NAME,
                                     PATIENT_LAST_NAME,
                                     PATIENT_GENDER,
                                     PATIENT_DOB,
                                     CATALOG_NUMBER,
                                     UNIT_SELLING_PRICE,
                                     SERIAL_NUMBER,
                                     LINE_ID,
                                     PRODUCT_CATEGORY,
                                     UDI_CATALOG,
                                     PRODUCT_DESCRIPTION,
                                     PATELLA_FLAG,
                                     SCHED_SURGERY_DATE,
                                     COMMITMENT_DATE,
                                     SURGERY_LOCATION,
                                     STATUS,
                                     IMPLANT_REQUEST_DATE,
                                     SCAN_RECEIVED_DATE,
                                     SCAN_CP_FLAG,
                                     SHIPPED_DATE,
                                     REGION_CODE,
                                     SALES_REP_NAME,
                                     TERRITORY_CODE,
                                     COUNTRY_CODE,
                                     --ACTUAL_SURGERY_DATE,    --Commented by 8.0
                                     RESCHEDULED_SURGERY_DATE, --Added by 8.0
                                     CURRENCY,
                                     HOSPITAL_NAME,
                                     SALESREP_NUMBER,
                                     LINE_TYPE
                                     ,IRF_ID
                                     ,PRODUCT_DESC_ORDCNF
                                     ,ORDER_DETAILS_ORDCNF
                                     ,ORDER_DETAILS2  --Added 7.0
                                     ,ACTUAL_SURGERY_DATE --Added by 8.0
                                     )
              VALUES (l_data1(x1).ORDER_ID,
                                     l_data1(x1).SURGEON_ID,
                                     l_data1(x1).SURGEON_TITLE,
                                     l_data1(x1).SURGEON_LAST_NAME,
                                     l_data1(x1).SURGEON_FIRST_NAME,
                                     l_data1(x1).PATIENT_FIRST_NAME,
                                     l_data1(x1).PATIENT_LAST_NAME,
                                     l_data1(x1).PATIENT_GENDER,
                                     l_data1(x1).PATIENT_DOB,
                                     l_data1(x1).CATALOG_NUMBER,
                                     l_data1(x1).UNIT_SELLING_PRICE,
                                     l_data1(x1).SERIAL_NUMBER,
                                     l_data1(x1).LINE_ID,
                                     l_data1(x1).PRODUCT_CATEGORY,
                                     l_data1(x1).UDI_CATALOG,
                                     l_data1(x1).PRODUCT_DESCRIPTION,
                                     l_data1(x1).PATELLA_FLAG,
                                     l_data1(x1).SCHED_SURGERY_DATE,
                                     l_data1(x1).COMMITMENT_DATE,
                                     l_data1(x1).SURGERY_LOCATION,
                                     l_data1(x1).status,
                                     l_data1(x1).IMPLANT_REQUEST_DATE,
                                     l_data1(x1).SCAN_RECEIVED_DATE,
                                     l_data1(x1).SCAN_CP_FLAG,
                                     l_data1(x1).SHIPPED_DATE,
                                     l_data1(x1).REGION_CODE,
                                     l_data1(x1).SALES_REP_NAME,
                                     l_data1(x1).TERRITORY_CODE,
                                     l_data1(x1).COUNTRY_CODE,
                                     --l_data1(x1).ACTUAL_SURGERY_DATE,      --Commented by 8.0
                                     l_data1(x1).RESCHEDULED_SURGERY_DATE, --Added by 8.0
                                     l_data1(x1).CURRENCY,
                                     l_data1(x1).HOSPITAL_NAME,
                                     l_data1(x1).SALESREP_NUMBER,
                                     l_data1(x1).LINE_TYPE,
                                     l_data1(x1).IRF_ID
                                     ,l_data1(x1).PRODUCT_DESC_ORDCNF
                                     ,l_data1(x1).ORDER_DETAILS_ORDCNF
                                     ,l_data1(x1).ORDER_DETAILS_2_ORDCNF --Added 7.0
                                     ,l_data1(x1).ACTUAL_SURGERY_DATE      --Added by 8.0
                                     );

               l_cnt := l_cnt + 1;
    exit when G3_ORDER_C%notfound;
    end loop;
    fnd_file.put_line(fnd_file.log, 'After G3 Record Insert. ' || current_timestamp);
    --Added End V5.0

     FND_FILE.PUT_LINE (FND_FILE.LOG,'Total Number of Records inserted : '||l_cnt);
     COMMIT;
      FND_FILE.PUT_LINE (FND_FILE.LOG,'End Concurrent Excution... ' || CURRENT_TIMESTAMP); --Added V5.0
   FND_FILE.PUT_LINE (
      FND_FILE.LOG,'-------------------------End Populate Orders table------------------------------------------------------------------');

        EXCEPTION
           WHEN OTHERS
           THEN
              FND_FILE.PUT_LINE (FND_FILE.LOG,'ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
        END;
END XXCMIS_ORD_CONF_PKG;