--create materialized view log on ONT.OE_ORDER_LINES_ALL with rowid;
create materialized view CONFORDERS.XXCMIS_CONF_ORD_MV -- replacing XXCMIS.XXCMIS_ORD_CONF
    parallel
    build immediate
    refresh force
		next sysdate + 1/24/20
    as 
with order_detail as (
	select
		ola.ROWID as ORDER_LINE_ROWID,
		ola.HEADER_ID,
		ola.LINE_ID,
		ola.LINE_TYPE_ID,
		ola.INVENTORY_ITEM_ID,
		ola.FLOW_STATUS_CODE,
		ola.INVOICED_QUANTITY,
		ola.UNIT_SELLING_PRICE,
		ola.ATTRIBUTE1,
		ola.REQUEST_DATE,
		ola.PROMISE_DATE,
		ola.SCHEDULE_SHIP_DATE,
		ola.ACTUAL_SHIPMENT_DATE,  -- logic changed		
		ola.ATTRIBUTE4,
		ola.ATTRIBUTE7,
		ola.SHIP_TO_ORG_ID,
		ola.SALESREP_ID,
		ola.ORG_ID,
		msi.CATALOG,
		msi.ITEM_NUMBER,
		msi.ITEM_DESCRIPTION,
		ola.ORDERED_ITEM,
		msi.ORDCNF_FLAG,
		max(msi.ORDCNF_FLAG) over (partition by ola.HEADER_ID) as ORDER_ORDCNF_FLAG
	from
			APPS.OE_ORDER_LINES_ALL ola 
		left outer join
			APPS.XXCMIS_UNI_ITEM_V msi on ola.INVENTORY_ITEM_ID = msi.INVENTORY_ITEM_ID
)
SELECT 
	oha.ORDER_NUMBER as ORDER_ID,
	surgeon.PERSON_PARTY_ID as SURGEON_ID, -- surgeon logic changed
	surgeon.PERSON_PRE_NAME_ADJUNCT as SURGEON_TITLE,
	surgeon.PERSON_LAST_NAME as SURGEON_LAST_NAME,
	surgeon.PERSON_FIRST_NAME as SURGEON_FIRST_NAME,
	oha.ATTRIBUTE2 as PATIENT_FIRST_NAME,
	oha.ATTRIBUTE1 as PATIENT_LAST_NAME,
	oha.ATTRIBUTE3 as PATIENT_GENDER,
	to_char(to_date(substr(oha.ATTRIBUTE4,1,10), 'YYYY/MM/DD'), 'DD-MON-YYYY') as PATIENT_DOB,
	replace(ola.CATALOG,'-','') as CATALOG_NUMBER,
	ola.UNIT_SELLING_PRICE,
	ola.ATTRIBUTE1 as SERIAL_NUMBER,
	ola.LINE_ID as LINE_ID,
	nvl(msi_lkup.MEANING, 'NOCAT') as PRODUCT_CATEGORY,
	replace(ola.CATALOG,'-','') as UDI_CATALOG,
	ola.ITEM_DESCRIPTION as PRODUCT_DESCRIPTION,
	case when msi_lkup.MEANING like 'PATELLA%' then 'True' else 'False' end as PATELLA_FLAG,
	ola.PROMISE_DATE as SCHED_SURGERY_DATE,
	ola.SCHEDULE_SHIP_DATE as COMMITMENT_DATE,
	nvl2(hospital.PS_ADDRESS1, hospital.PS_ADDRESS1 || ', ', '') ||
		nvl2(hospital.PS_ADDRESS2, hospital.PS_ADDRESS2 || ', ', '') ||
		nvl2(hospital.PS_ADDRESS3, hospital.PS_ADDRESS3 || ', ', '') ||
		nvl2(hospital.PS_ADDRESS4, hospital.PS_ADDRESS4 || ', ', '') ||
		nvl2(hospital.PS_CITY, hospital.PS_CITY || ', ', '') ||
		nvl(hospital.PS_STATE, hospital.PS_PROVINCE) || ', ' ||
		nvl2(hospital.PS_COUNTRY, hospital.PS_COUNTRY || ', ', '') as SURGERY_LOCATION,
	case 
		when ola.FLOW_STATUS_CODE = 'CLOSED' and ola.INVOICED_QUANTITY <= 0 then 'RETURN' 
		when ola.FLOW_STATUS_CODE = 'PENDING_DESIGN_APPROVAL' then 
			decode(upper(rev.STATUS), 
				'PENDING', 'Pending Surgeon Review',
				'DECLINED', 'Re-Design',
				'APPROVED', 'Design Approved by Surgeon',
				ola.FLOW_STATUS_CODE)
		else ola.FLOW_STATUS_CODE end as STATUS, -- logic changed, will check
	ola.REQUEST_DATE as IMPLANT_REQUEST_DATE,
	pacs.RECEIVED_DATE as SCAN_RECEIVED_DATE,
	pacs.USER5 as SCAN_CP_FLAG, -- this is changed, orginal was max() in SERIAL_NUMBER
	ola.ACTUAL_SHIPMENT_DATE as SHIPPED_DATE,  -- logic changed
	hospital.REGION as REGION_CODE,
	reps.SALESREP_NAME as SALES_REP_NAME,
	hospital.TERRITORY as TERRITORY_CODE,
	hospital.COUNTRY as COUNTRY_CODE,
	to_date(substr(ola.ATTRIBUTE4, 1,10), 'YYYY/MM/DD') as RESCHEDULED_SURGERY_DATE,
	oha.TRANSACTIONAL_CURR_CODE as CURRENCY,
	hospital.PARTY_NAME as HOSPITAL_NAME,
	nvl(reps.SALESREP_NUMBER, 'X') AS SALESREP_NUMBER,
	ott.NAME as LINE_TYPE,
	to_number(oha.ATTRIBUTE9) as IRF_ID,
	case when ola.ORDER_ORDCNF_FLAG = 1 then oha.ATTRIBUTE10 else null end as PRODUCT_DESC_ORDCNF,
	case when ola.ORDER_ORDCNF_FLAG = 1 then oha.ATTRIBUTE11 else null end as ORDER_DETAILS_ORDCNF,
	case when ola.ORDER_ORDCNF_FLAG = 1 then oha.ATTRIBUTE17 else null end as ORDER_DETAILS2,
	to_date(substr(ola.ATTRIBUTE7, 1,10), 'YYYY/MM/DD') as ACTUAL_SURGERY_DATE,
	/*** extra fields ***/ 
	regexp_replace(ola.ATTRIBUTE1, '^[AB]', '0') as SERIAL_NUMBER_ORI,
	ola.ORDER_LINE_ROWID,
	oha.HEADER_ID,
	ola.INVENTORY_ITEM_ID,
	ola.ORDERED_ITEM,
	ola.ITEM_NUMBER,
	ola.ORDCNF_FLAG,
	ola.ORDER_ORDCNF_FLAG
from
        APPS.OE_ORDER_HEADERS_ALL oha
    left outer join
		order_detail ola on oha.HEADER_ID = ola.HEADER_ID
	left outer join
		APPS.OE_TRANSACTION_TYPES_TL ott on ola.LINE_TYPE_ID = ott.TRANSACTION_TYPE_ID
	left outer join
		APPS.XXCMIS_UNI_CONTACT_V surgeon on oha.SOLD_TO_CONTACT_ID = surgeon.CUST_ACCOUNT_ROLE_ID
	left outer join
		APPS.XXCMIS_UNI_SITE_USE_V hospital on ola.SHIP_TO_ORG_ID = hospital.SITE_USE_ID
	left outer join
		APPS.XXCMIS_UNI_US_SALESREP_V reps on ola.SALESREP_ID = reps.SALESREP_ID and ola.ORG_ID = reps.SALESREP_ORG_ID
	left outer join
		APPS.FND_LOOKUP_VALUES msi_lkup 
		on msi_lkup.LANGUAGE = userenv('LANG') and msi_lkup.LOOKUP_TYPE = 'CNFXX_PRODUCT_CAT'
            AND msi_lkup.VIEW_APPLICATION_ID = 401
            AND msi_lkup.SECURITY_GROUP_ID = 0
            AND substr (ola.ITEM_NUMBER, 1, 5) = msi_lkup.LOOKUP_CODE
	left outer join
		APPS.XXCMIS_UNI_PACS_V pacs on pacs.USER2 = ola.ATTRIBUTE1 and pacs.RECENCY_RECEIVED_D = 1
	left outer join
		APPS.XXCMIS_DRD_HIP_REVISION_V rev on rev.SERIAL_NUMBER = ola.ATTRIBUTE1 and rev.REVISION_REVERSE = 1
where
	surgeon.PERSON_PARTY_ID is not null
	and ott.NAME not in ('HIP Procedure','HIP Alt Kit to Sales Rep','HIP RMA Alt Kit SRep Rec Only', 'HIP RMA Proc Cust Rec ',
                               'HIP Alt Kit Item to Customer', 'HIP Alt Kit to Sales Rep','HIP Patient Specific')
	and oha.ORDER_NUMBER < 700001  -- this might expire soon
	and (ola.ORDCNF_FLAG = 1 or ola.ORDER_ORDCNF_FLAG = 0) -- for G3 Orders, only take the flaged item.
	and nvl(msi_lkup.MEANING, 'NOCAT') not like 'PATELLA%'
	and 
		( 	ola.FLOW_STATUS_CODE not in ('CLOSED', 'CANCELLED')
			or (ola.FLOW_STATUS_CODE = 'CLOSED' and nvl(ola.INVOICED_QUANTITY,1) > 0 ) --and ola.ACTUAL_SHIPMENT_DATE >= sysdate - 30)
			or (ola.FLOW_STATUS_CODE = 'CLOSED' and ola.INVOICED_QUANTITY <= 0 and ola.SCHEDULE_SHIP_DATE >= sysdate - 30)
			or (ola.FLOW_STATUS_CODE = 'CANCELLED' and ola.SCHEDULE_SHIP_DATE >= sysdate - 30)
		)
;

create index CONFORDERS.CONFVIEW_ORDER_ID_IDX on CONFORDERS.XXCMIS_CONF_ORD_MV(ORDER_ID) tablespace CONFORMIS;
create unique index CONFORDERS.CONFVIEW_LINE_ID_IDX on CONFORDERS.XXCMIS_CONF_ORD_MV(LINE_ID) tablespace CONFORMIS;
create index CONFORDERS.CONFVIEW_SALESREP_NUMBER_IDX on CONFORDERS.XXCMIS_CONF_ORD_MV(SALESREP_NUMBER) tablespace CONFORMIS;
create index CONFORDERS.CONFVIEW_SERIAL_NUMBER_IDX on CONFORDERS.XXCMIS_CONF_ORD_MV(SERIAL_NUMBER) tablespace CONFORMIS;
create index CONFORDERS.CONFVIEW_SERIAL_NUMBER_ORI_IDX on CONFORDERS.XXCMIS_CONF_ORD_MV(SERIAL_NUMBER_ORI) tablespace CONFORMIS;
create index CONFORDERS.CONFVIEW_STATUS_IDX on CONFORDERS.XXCMIS_CONF_ORD_MV(STATUS) tablespace CONFORMIS;
create index CONFORDERS.CONFVIEW_SURGEON_ID_IDX on CONFORDERS.XXCMIS_CONF_ORD_MV(SURGEON_ID) tablespace CONFORMIS;


