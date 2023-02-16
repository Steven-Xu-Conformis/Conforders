/** to do **
 * 1. change table
 *		a. add all new columns to XXCMIS.XXCMIS_CONF_ORD
 *		b. add one new index to XXCMIS.XXCMIS_CONF_ORD
 * 2. change package
 *		a. replace XXCMIS_ORD_CONF_PKG_TEST with XXCMIS_ORD_CONF_PKG (4)
 *		b. replace XXCMIS_CONF_ORD_TN with XXCMIS_CONF_ORD (4)
 *		c. comment out all DMBS_OUTPUT
 */ 

create or replace package      XXCMIS_ORD_CONF_PKG
as
/******************************************************************************
 NAME:         APPS.XXCMIS_ORD_CONF_PKG
 PURPOSE:   To populate the table for orders.conformis to fetch the order data

   REVISIONS:
   Ver               Date                 Author                  Description
   ---------        --------------      ---------------         ------------------------------------
   1.0              14-FEB-2018       	Priya H              	Initial Version
   9.0				21-OCT-2020			Steven Xu				Added two procedures
*******************************************************************************/

	PKB_RESP_ID      number := fnd_global.RESP_ID;
	PKB_RESP_APPL_ID number := fnd_global.RESP_APPL_ID;
	PKB_USER_ID      number := fnd_global.USER_ID;
	PKB_SYSDATE      date   := sysdate; 
      
	procedure MAIN (RETCODE out number, ERRBUF out varchar2);
	--old process						-- dev: 450 seconds		prod: 242 seconds
	procedure REFRESH_ORD_CONF;			-- dev: 182 seconds		prod: 176 seconds
	procedure INCREMENT_ORD_CONF;		-- dev: 80 seconds		prod: 18 seconds
                                      
end XXCMIS_ORD_CONF_PKG;


create or replace package body      XXCMIS_ORD_CONF_PKG
as
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
   9.0				15-Oct-2020		  Steven Xu			   rewrite the full logic. Change to incremental logic
*******************************************************************************/

	Function GET_HIP_REV_STATUS( l_serial_number varchar2) 
		return varchar 
	is
		l_status varchar2(100);
	begin
		select status into l_status
		from conforders.hip_revision
		where 
			serial_number = l_serial_number
			and revision = 
				(
					select max(revision)
					from conforders.hip_revision
					where serial_number = l_serial_number
				)
		;
		return upper(l_status);
		
		exception when others then
			l_status := null;
			return upper(l_status);
	end GET_HIP_REV_STATUS;
	
	procedure REFRESH_ORD_CONF is  
		begin
			FND_FILE.PUT_LINE (FND_FILE.LOG,'Start deleting: ' || current_timestamp);
			--DBMS_OUTPUT.PUT_LINE ('Start deleting: ' || current_timestamp);
			
			delete from XXCMIS.XXCMIS_CONF_ORD;
		
			--DBMS_OUTPUT.PUT_LINE ('Deleted all (' || sql%rowcount || ') records from order table. ' || current_timestamp);
			FND_FILE.PUT_LINE (FND_FILE.LOG,'Deleted all (' || sql%rowcount || ') records from order table. ' || current_timestamp);
			
			insert into XXCMIS.XXCMIS_CONF_ORD 
			select * From APPS.XXCMIS_CONF_ORD_V2;
			
			--DBMS_OUTPUT.PUT_LINE ('Inserted ' || sql%rowcount || ' records. ' || current_timestamp);
			FND_FILE.PUT_LINE (FND_FILE.LOG,'Inserted ' || sql%rowcount || ' records. ' || current_timestamp);

			COMMIT;	
		exception
			when others then
				--DBMS_OUTPUT.PUT_LINE ('ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
				FND_FILE.PUT_LINE (FND_FILE.LOG,'ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
				rollback;
		end;	

	procedure INCREMENT_ORD_CONF is  -- dev: 80 seconds
		begin
			FND_FILE.PUT_LINE (FND_FILE.LOG,'Start deleting: ' || current_timestamp);
			--DBMS_OUTPUT.PUT_LINE ('Start deleting: ' || current_timestamp);
			
			delete from 
				XXCMIS.XXCMIS_CONF_ORD
			where 
				LINE_ID not in (select LINE_ID FROM APPS.XXCMIS_CONF_ORD_V2)
--				(STATUS = 'RETURN' and COMMITMENT_DATE < sysdate - 30) or
--				(STATUS = 'CANCELLED' and COMMITMENT_DATE < sysdate - 30)
			;
		
			--DBMS_OUTPUT.PUT_LINE ('Deleted ' || sql%rowcount || ' old records of CANCELLED and RETURN orders. ' || current_timestamp);
			FND_FILE.PUT_LINE (FND_FILE.LOG,'Deleted ' || sql%rowcount || ' old records of CANCELLED and RETURN orders. ' || current_timestamp);
			
			merge into 
				XXCMIS.XXCMIS_CONF_ORD tgt
			using 
				(
					select 
						*
					from 
						APPS.XXCMIS_CONF_ORD_V2
					where
						LAST_UPDATE_DATE > sysdate - 1/24 or 
						SCAN_RECEIVED_DATE > sysdate - 14 or
						DATE_SUBMITTED > sysdate - 7
				) src
			on 
				(src.LINE_ID = tgt.LINE_ID)
			when matched then
				update set
					tgt.ORDER_ID = src.ORDER_ID,
					tgt.SURGEON_ID = src.SURGEON_ID,
					tgt.SURGEON_TITLE = src.SURGEON_TITLE,
					tgt.SURGEON_LAST_NAME = src.SURGEON_LAST_NAME,
					tgt.SURGEON_FIRST_NAME = src.SURGEON_FIRST_NAME,
					tgt.PATIENT_FIRST_NAME = src.PATIENT_FIRST_NAME,
					tgt.PATIENT_LAST_NAME = src.PATIENT_LAST_NAME,
					tgt.PATIENT_GENDER = src.PATIENT_GENDER,
					tgt.PATIENT_DOB = src.PATIENT_DOB,
					tgt.CATALOG_NUMBER = src.CATALOG_NUMBER,
					tgt.UNIT_SELLING_PRICE = src.UNIT_SELLING_PRICE,
					tgt.SERIAL_NUMBER = src.SERIAL_NUMBER,
					--tgt.LINE_ID = src.LINE_ID,
					tgt.PRODUCT_CATEGORY = src.PRODUCT_CATEGORY,
					tgt.UDI_CATALOG = src.UDI_CATALOG,
					tgt.PRODUCT_DESCRIPTION = src.PRODUCT_DESCRIPTION,
					tgt.PATELLA_FLAG = src.PATELLA_FLAG,
					tgt.SCHED_SURGERY_DATE = src.SCHED_SURGERY_DATE,
					tgt.COMMITMENT_DATE = src.COMMITMENT_DATE,
					tgt.SURGERY_LOCATION = src.SURGERY_LOCATION,
					tgt.STATUS = src.STATUS,
					tgt.IMPLANT_REQUEST_DATE = src.IMPLANT_REQUEST_DATE,
					tgt.SCAN_RECEIVED_DATE = src.SCAN_RECEIVED_DATE,
					tgt.SCAN_CP_FLAG = src.SCAN_CP_FLAG,
					tgt.SHIPPED_DATE = src.SHIPPED_DATE,
					tgt.REGION_CODE = src.REGION_CODE,
					tgt.SALES_REP_NAME = src.SALES_REP_NAME,
					tgt.TERRITORY_CODE = src.TERRITORY_CODE,
					tgt.COUNTRY_CODE = src.COUNTRY_CODE,
					tgt.RESCHEDULED_SURGERY_DATE = src.RESCHEDULED_SURGERY_DATE,
					tgt.CURRENCY = src.CURRENCY,
					tgt.HOSPITAL_NAME = src.HOSPITAL_NAME,
					tgt.SALESREP_NUMBER = src.SALESREP_NUMBER,
					tgt.LINE_TYPE = src.LINE_TYPE,
					tgt.IRF_ID = src.IRF_ID,
					tgt.PRODUCT_DESC_ORDCNF = src.PRODUCT_DESC_ORDCNF,
					tgt.ORDER_DETAILS_ORDCNF = src.ORDER_DETAILS_ORDCNF,
					tgt.ORDER_DETAILS2 = src.ORDER_DETAILS2,
					tgt.ACTUAL_SURGERY_DATE = src.ACTUAL_SURGERY_DATE,
					tgt.SERIAL_NUMBER_ORI = src.SERIAL_NUMBER_ORI,
					tgt.ORDER_LINE_ROWID = src.ORDER_LINE_ROWID,
					tgt.HEADER_ID = src.HEADER_ID,
					tgt.INVENTORY_ITEM_ID = src.INVENTORY_ITEM_ID,
					tgt.ORDERED_ITEM = src.ORDERED_ITEM,
					tgt.ITEM_NUMBER = src.ITEM_NUMBER,
					tgt.ORDCNF_FLAG = src.ORDCNF_FLAG,
					tgt.ORDER_ORDCNF_FLAG = src.ORDER_ORDCNF_FLAG,
					tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
					tgt.SOLD_TO_CONTACT_ID = src.SOLD_TO_CONTACT_ID,
					tgt.SHIP_TO_ORG_ID = src.SHIP_TO_ORG_ID,
					tgt.SALESREP_ID = src.SALESREP_ID,
					tgt.DATE_SUBMITTED = src.DATE_SUBMITTED			
				where
					src.LAST_UPDATE_DATE > tgt.LAST_UPDATE_DATE or
					nvl(src.SCAN_RECEIVED_DATE,to_date(20000101,'yyyymmdd')) != nvl(tgt.SCAN_RECEIVED_DATE,to_date(20000101,'yyyymmdd')) or
					nvl(src.DATE_SUBMITTED,to_date(20000101,'yyyymmdd')) != nvl(tgt.DATE_SUBMITTED,to_date(20000101,'yyyymmdd'))
			when not matched then 	
				insert (
					ORDER_ID,
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
					RESCHEDULED_SURGERY_DATE,
					CURRENCY,
					HOSPITAL_NAME,
					SALESREP_NUMBER,
					LINE_TYPE,
					IRF_ID,
					PRODUCT_DESC_ORDCNF,
					ORDER_DETAILS_ORDCNF,
					ORDER_DETAILS2,
					ACTUAL_SURGERY_DATE,
					SERIAL_NUMBER_ORI,
					ORDER_LINE_ROWID,
					HEADER_ID,
					INVENTORY_ITEM_ID,
					ORDERED_ITEM,
					ITEM_NUMBER,
					ORDCNF_FLAG,
					ORDER_ORDCNF_FLAG,
					LAST_UPDATE_DATE,
					SOLD_TO_CONTACT_ID,
					SHIP_TO_ORG_ID,
					SALESREP_ID,
					DATE_SUBMITTED
				)
				values (
					src.ORDER_ID,
					src.SURGEON_ID,
					src.SURGEON_TITLE,
					src.SURGEON_LAST_NAME,
					src.SURGEON_FIRST_NAME,
					src.PATIENT_FIRST_NAME,
					src.PATIENT_LAST_NAME,
					src.PATIENT_GENDER,
					src.PATIENT_DOB,
					src.CATALOG_NUMBER,
					src.UNIT_SELLING_PRICE,
					src.SERIAL_NUMBER,
					src.LINE_ID,
					src.PRODUCT_CATEGORY,
					src.UDI_CATALOG,
					src.PRODUCT_DESCRIPTION,
					src.PATELLA_FLAG,
					src.SCHED_SURGERY_DATE,
					src.COMMITMENT_DATE,
					src.SURGERY_LOCATION,
					src.STATUS,
					src.IMPLANT_REQUEST_DATE,
					src.SCAN_RECEIVED_DATE,
					src.SCAN_CP_FLAG,
					src.SHIPPED_DATE,
					src.REGION_CODE,
					src.SALES_REP_NAME,
					src.TERRITORY_CODE,
					src.COUNTRY_CODE,
					src.RESCHEDULED_SURGERY_DATE,
					src.CURRENCY,
					src.HOSPITAL_NAME,
					src.SALESREP_NUMBER,
					src.LINE_TYPE,
					src.IRF_ID,
					src.PRODUCT_DESC_ORDCNF,
					src.ORDER_DETAILS_ORDCNF,
					src.ORDER_DETAILS2,
					src.ACTUAL_SURGERY_DATE,
					src.SERIAL_NUMBER_ORI,
					src.ORDER_LINE_ROWID,
					src.HEADER_ID,
					src.INVENTORY_ITEM_ID,
					src.ORDERED_ITEM,
					src.ITEM_NUMBER,
					src.ORDCNF_FLAG,
					src.ORDER_ORDCNF_FLAG,
					src.LAST_UPDATE_DATE,
					src.SOLD_TO_CONTACT_ID,
					src.SHIP_TO_ORG_ID,
					src.SALESREP_ID,
					src.DATE_SUBMITTED
				)
			;
			
			--DBMS_OUTPUT.PUT_LINE ('Merged ' || sql%rowcount || ' records. ' || current_timestamp);
			FND_FILE.PUT_LINE (FND_FILE.LOG,'Merged ' || sql%rowcount || ' records. ' || current_timestamp);
			COMMIT;
			
		exception
			when others then
				--DBMS_OUTPUT.PUT_LINE ('ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
				FND_FILE.PUT_LINE (FND_FILE.LOG,'ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
				rollback;
		end;		

	procedure MAIN(
		RETCODE       out number,
		ERRBUF        out varchar2) 
	is
		begin
			FND_FILE.PUT_LINE (FND_FILE.LOG,'=========Calling populate order table proc=============');
			--DBMS_OUTPUT.PUT_LINE ('=========Calling populate order table proc=============:' || current_timestamp);

			REFRESH_ORD_CONF();

			--DBMS_OUTPUT.PUT_LINE ('End Concurrent Excution... ' || current_timestamp);
			--DBMS_OUTPUT.PUT_LINE ('==========END of populate order table proc=============:' || current_timestamp);
			FND_FILE.PUT_LINE (FND_FILE.LOG,'=========END of populate order table proc=============');
			
		exception
			when others then
				--DBMS_OUTPUT.PUT_LINE ('ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
				FND_FILE.PUT_LINE (FND_FILE.LOG,'ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
		end;
end XXCMIS_ORD_CONF_PKG;