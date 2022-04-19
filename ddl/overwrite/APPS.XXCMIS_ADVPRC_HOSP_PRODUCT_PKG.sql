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
   1.2				12-APR-2022			Steven Xu				Changed logic to include OUS hospitals
																break the query into 2 parts for performance
*******************************************************************************/
	procedure REFRESH_HOSP_PRODUCT is  
			v_in_clause varchar2(4000);
            v_full_sql varchar2(4000);
			v_update_clause varchar2(4000);
			v_update_sql varchar2(4000);
			v_pre_sql varchar2(4000);
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
				
			v_pre_sql := q'{
create table XXCMIS_ADVPRC_PRICELIST_ITEM as
select
    qll.LIST_HEADER_ID as PRICE_LIST_ID,
	qpa.PRODUCT_ATTRIBUTE,
    msib.INVENTORY_ITEM_ID
--    ordcnf_cat.SEGMENT1 as CATEGORY
from
	QP_LIST_LINES qll
	join QP_PRICING_ATTRIBUTES qpa on qll.LIST_LINE_ID = qpa.LIST_LINE_ID
	left outer join MTL_ITEM_CATEGORIES mic on 
		case when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE2' then to_number(qpa.PRODUCT_ATTR_VALUE) end = mic.CATEGORY_ID
		and mic.ORGANIZATION_ID = 84
	left outer join MTL_SYSTEM_ITEMS_B msib on 
		case 
			when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE1' then to_number(qpa.PRODUCT_ATTR_VALUE)
			when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE2' then mic.INVENTORY_ITEM_ID
			--when qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE3' then msib.INVENTORY_ITEM_ID 
		end = msib.INVENTORY_ITEM_ID 
		and msib.ORGANIZATION_ID = 84
where
	(SYSDATE) BETWEEN NVL((QLL.START_DATE_ACTIVE),(SYSDATE)) AND NVL((QLL.END_DATE_ACTIVE),(SYSDATE+1))
	AND (qpa.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE3' or msib.SEGMENT1 is not null)			
			}';
			
			v_full_sql := q'{
create table XXCMIS_TEMP_ADVPRC as
with PARTY_GROUPING as (
	select
		GROUP_HP.PARTY_ID as GROUP_PARTY_ID,
		HP.PARTY_ID as HOSPITAL_ID,
		HP.PARTY_NAME as HOSPITAL
	from
        HZ_PARTIES HP 
        left outer join HZ_RELATIONSHIPS HR  on 
                HR.SUBJECT_ID = HP.PARTY_ID
                and HR.RELATIONSHIP_CODE = 'PARTICIPATING' 
				and HR.END_DATE >= sysdate
				and HR.STATUS ='A'
		left outer join HZ_PARTIES GROUP_HP on GROUP_HP.PARTY_ID = HR.OBJECT_ID
		left outer join HZ_CUST_ACCOUNTS HCA on 
			GROUP_HP.PARTY_ID = HCA.PARTY_ID -- for group account status
			and HCA.CUSTOMER_CLASS_CODE is not NULL
			and HCA.STATUS = 'A'
), PARTY_PRICE_LIST as (
	select distinct
		pg.GROUP_PARTY_ID,
        pg.HOSPITAL_ID,
        pg.HOSPITAL,
		nvl(adv_prc.GRP_PRICE_LIST_ID, site_use.PRICE_LIST_ID) as PRICE_LIST_ID,
		loc.COUNTRY
	from
		PARTY_GROUPING pg
		left outer join XXCMIS_ADVPRICE_GRP_PRECEDENCE adv_prc on 
            (adv_prc.GRP_PARTY_ID = pg.GROUP_PARTY_ID or adv_prc.CUST_PARTY_ID = pg.HOSPITAL_ID) 
            and adv_prc.ACTIVE = 'YES'
            AND (SYSDATE) BETWEEN NVL((ADV_PRC.START_DATE),(SYSDATE)) AND NVL((ADV_PRC.END_DATE),(SYSDATE+1))
		join HZ_PARTY_SITES ps on ps.PARTY_ID = pg.HOSPITAL_ID	
		join HZ_LOCATIONS loc on ps.LOCATION_ID = loc.LOCATION_ID
		join HZ_CUST_ACCT_SITES_ALL cas on cas.PARTY_SITE_ID = ps.PARTY_SITE_ID
		join HZ_CUST_SITE_USES_ALL site_use on site_use.CUST_ACCT_SITE_ID = cas.CUST_ACCT_SITE_ID
)
select 
	*
from (
	select
		adv_prc.HOSPITAL_ID as HOSPITAL_ID,
		adv_prc.HOSPITAL as HOSPITAL,
        ordcnf_cat.SEGMENT1 as CATEGORY
	from 
		PARTY_PRICE_LIST adv_prc
		join XXCMIS_ADVPRC_PRICELIST_ITEM pi on adv_prc.price_list_id = pi.price_list_id
		join MTL_ITEM_CATEGORIES ordcnf on 
			ordcnf.category_set_id = 1100000201 
			and ordcnf.ORGANIZATION_ID = 84 
			and case when pi.PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE3' then ordcnf.INVENTORY_ITEM_ID else pi.INVENTORY_ITEM_ID end = ordcnf.INVENTORY_ITEM_ID
		join MTL_CATEGORIES_B ordcnf_cat on ordcnf_cat.CATEGORY_ID = ordcnf.CATEGORY_ID
)
pivot (
	max(case when CATEGORY is not null then 1 else 0 end)
	for CATEGORY in (}' || v_in_clause || '))';
            --DBMS_OUTPUT.put_line(v_full_sql);
			begin
				execute immediate 'drop table XXCMIS_ADVPRC_PRICELIST_ITEM';
            exception 
                when others then 
                    if SQLCODE != -942 then
					raise;
				end if;
            end;
			execute immediate v_pre_sql;
			execute immediate 'create index XXCMIS_ADVPRC_PRICELIST_ITEM_pl on XXCMIS_ADVPRC_PRICELIST_ITEM (price_list_id)';
			execute immediate 'create index XXCMIS_ADVPRC_PRICELIST_ITEM_item on XXCMIS_ADVPRC_PRICELIST_ITEM (inventory_item_id)';
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

