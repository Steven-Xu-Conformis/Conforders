create or replace package           XXCMIS_ADVPRC_HOSP_PRODUCT_PKG
as
/******************************************************************************
 NAME:         APPS.XXCMIS_ADVPRC_HOSP_PRODUCT_PKG
 PURPOSE:   To populate the table for orders.conformis to fetch the product list for hospitals

   REVISIONS:
   Ver               Date                 Author                  Description
   ---------        --------------      ---------------         ------------------------------------
   1.0              11-NOV-2021       	Steven Xu             	Initial Version
*******************************************************************************/
	PKB_RESP_ID      number := fnd_global.RESP_ID;
	PKB_RESP_APPL_ID number := fnd_global.RESP_APPL_ID;
	PKB_USER_ID      number := fnd_global.USER_ID;
	PKB_SYSDATE      date   := sysdate; 

	procedure MAIN (RETCODE out number, ERRBUF out varchar2);
	procedure REFRESH_HOSP_PRODUCT;	

end XXCMIS_ADVPRC_HOSP_PRODUCT_PKG;

create or replace package body           XXCMIS_ADVPRC_HOSP_PRODUCT_PKG
as
/******************************************************************************
 NAME:         APPS.XXCMIS_ADVPRC_HOSP_PRODUCT_PKG
 PURPOSE:      To populate the table for orders.conformis to fetch the product list for hospitals

   REVISIONS:
   Ver               Date                 Author                  Description
   ---------        --------------      ---------------         ------------------------------------
   1.0              11-NOV-2021       	Steven Xu             	Initial Version
   1.1				11-MAR-2022			Steven Xu				Added update query to set all nulls to 0
*******************************************************************************/
	procedure REFRESH_HOSP_PRODUCT is  
			v_in_clause varchar2(4000);
            v_full_sql varchar2(4000);
			v_update_clause varchar2(4000);
			v_update_sql varchar2(4000);
		begin
			select
				listagg('''' || SEGMENT1 || ''' as ' || SEGMENT1, ',')  WITHIN GROUP (ORDER BY SEGMENT1) into v_in_clause
			FROM
				(
					select distinct ordcnf_cat.SEGMENT1 from 
						MTL_ITEM_CATEGORIES ordcnf
						join MTL_CATEGORIES_B ordcnf_cat on ordcnf_cat.CATEGORY_ID = ordcnf.CATEGORY_ID
					where
						ordcnf.category_set_id = 1100000201 and ordcnf.ORGANIZATION_ID = 84
				);

			select
				listagg(SEGMENT1 || ' = nvl(' || SEGMENT1 || ',0)', ',')  WITHIN GROUP (ORDER BY SEGMENT1) into v_update_clause
			FROM
				(
					select distinct ordcnf_cat.SEGMENT1 from 
						MTL_ITEM_CATEGORIES ordcnf
						join MTL_CATEGORIES_B ordcnf_cat on ordcnf_cat.CATEGORY_ID = ordcnf.CATEGORY_ID
					where
						ordcnf.category_set_id = 1100000201 and ordcnf.ORGANIZATION_ID = 84
				);
			v_update_sql := q'{
				update XXCMIS_TEMP_ADVPRC set
				}' || v_update_clause;
				
			 v_full_sql := q'{
create table XXCMIS_TEMP_ADVPRC as
with PARTY_GROUPING as (
	select
		GROUP_HP.PARTY_ID as GROUP_PARTY_ID,
		HP.PARTY_ID as CUST_PARTY_ID,
		HP.PARTY_NAME as CUSTOMER_NAME
	from
		HZ_PARTIES GROUP_HP
		join HZ_CUST_ACCOUNTS HCA on GROUP_HP.PARTY_ID = HCA.PARTY_ID -- for group account status
		join HZ_RELATIONSHIPS HR on GROUP_HP.PARTY_ID = HR.OBJECT_ID
		join HZ_PARTIES HP on HR.SUBJECT_ID = HP.PARTY_ID
		join FND_LOOKUP_VALUES FLV on HR.RELATIONSHIP_CODE = FLV.LOOKUP_CODE and FLV.LOOKUP_TYPE = 'PARTY_RELATIONS_TYPE'	
	where
		HCA.CUSTOMER_CLASS_CODE is not NULL
		and HCA.STATUS = 'A'
		and FLV.MEANING = 'Customer Participating'
		and FLV.ENABLED_FLAG = 'Y'
		and TRUNC(SYSDATE) BETWEEN TRUNC(NVL(FLV.START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(FLV.END_DATE_ACTIVE,SYSDATE))
		and HR.END_DATE >= TRUNC(SYSDATE)
		and HR.STATUS ='A'
)
select 
	*
from (

	select
		nvl(cust_hp.PARTY_ID, pg.CUST_PARTY_ID) as HOSPITAL_ID,
		nvl(cust_hp.PARTY_NAME, pg.CUSTOMER_NAME) as HOSPITAL,
		ordcnf_cat.SEGMENT1 as CATEGORY
	from 
		XXCMIS_ADVPRICE_GRP_PRECEDENCE adv_prc
		join QP_LIST_HEADERS_TL qlh on adv_prc.GRP_PRICE_LIST_ID = qlh.LIST_HEADER_ID
		join QP_LIST_LINES qll on qlh.LIST_HEADER_ID = qll.LIST_HEADER_ID
		join QP_PRICING_ATTRIBUTES qpa on qll.LIST_LINE_ID = qpa.LIST_LINE_ID
		left outer join MTL_ITEM_CATEGORIES mic on 
			case when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE2' then to_number(qpa.PRODUCT_ATTR_VALUE) end = mic.CATEGORY_ID
			and mic.ORGANIZATION_ID = 84
		left outer join MTL_SYSTEM_ITEMS_B msib on 
			case 
				when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE1' then to_number(qpa.PRODUCT_ATTR_VALUE)
				when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE2' then mic.INVENTORY_ITEM_ID
			end = msib.INVENTORY_ITEM_ID 
			and msib.ORGANIZATION_ID = 84
		left outer join PARTY_GROUPING pg on adv_prc.GRP_PARTY_ID = pg.GROUP_PARTY_ID
		left outer join HZ_PARTIES cust_hp on adv_prc.CUST_PARTY_ID = cust_hp.PARTY_ID
		join MTL_ITEM_CATEGORIES ordcnf on ordcnf.category_set_id = 1100000201 and ordcnf.ORGANIZATION_ID = 84 and
			(
				case 
					when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE1' then to_number(qpa.PRODUCT_ATTR_VALUE) 
					when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE2' then mic.INVENTORY_ITEM_ID 
					when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE3' then ordcnf.INVENTORY_ITEM_ID
				end = ordcnf.INVENTORY_ITEM_ID
			)
		join
			MTL_CATEGORIES_B ordcnf_cat on ordcnf_cat.CATEGORY_ID = ordcnf.CATEGORY_ID
	where
		adv_prc.ACTIVE = 'YES'
		AND TRUNC(SYSDATE) BETWEEN NVL(TRUNC(QLL.START_DATE_ACTIVE),TRUNC(SYSDATE)) AND NVL(TRUNC(QLL.END_DATE_ACTIVE),TRUNC(SYSDATE+1))
		AND TRUNC(SYSDATE) BETWEEN NVL(TRUNC(ADV_PRC.START_DATE),TRUNC(SYSDATE)) AND NVL(TRUNC(ADV_PRC.END_DATE),TRUNC(SYSDATE+1))
		AND (pg.GROUP_PARTY_ID is not null or cust_hp.PARTY_ID is not null)
		AND (qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE3' or msib.SEGMENT1 is not null)
)
pivot (
	max(case when CATEGORY is not null then 1 else 0 end)
	for CATEGORY in (}' || v_in_clause || '))';
            --DBMS_OUTPUT.put_line(v_full_sql);
            begin 
                execute immediate 'drop table XXCMIS_TEMP_ADVPRC';
            exception 
                when others then 
                    if SQLCODE != -942 then
					raise;
				end if;
            end;
			execute immediate v_full_sql;
			execute immediate 'alter table XXCMIS_TEMP_ADVPRC add primary key (HOSPITAL_ID)';
			execute immediate v_update_sql;
            begin
                execute immediate 'drop table XXCMIS_ADVPRC_HOSPITAL_PRODUCT';
            exception 
                when others then 
                    if SQLCODE != -942 then
					raise;
				end if;
            end;
            execute immediate 'rename XXCMIS_TEMP_ADVPRC to XXCMIS_ADVPRC_HOSPITAL_PRODUCT';
    end REFRESH_HOSP_PRODUCT;

	procedure MAIN (RETCODE out number, ERRBUF out varchar2) is 
		begin
			FND_FILE.PUT_LINE (FND_FILE.LOG,'=========Calling populate hospital-product table proc=============');
			--DBMS_OUTPUT.PUT_LINE ('=========Calling populate order table proc=============:' || current_timestamp);

			REFRESH_HOSP_PRODUCT();

			--DBMS_OUTPUT.PUT_LINE ('End Concurrent Excution... ' || current_timestamp);
			--DBMS_OUTPUT.PUT_LINE ('==========END of populate order table proc=============:' || current_timestamp);
			FND_FILE.PUT_LINE (FND_FILE.LOG,'=========END of populate hospital-product table proc=============');

		exception
			when others then
				--DBMS_OUTPUT.PUT_LINE ('ERROR XXCMIS_ORD_CONF_PKG Error:' || sqlerrm);
				FND_FILE.PUT_LINE (FND_FILE.LOG,'ERROR XXCMIS_ADVPRC_HOSP_PRODUCT_PKG Error:' || sqlerrm);
		end MAIN;
end XXCMIS_ADVPRC_HOSP_PRODUCT_PKG;

