create or replace force view CONFORDERS.XXCMIS_CONF_ORD_IVIEW_DOF_V0 as
select distinct
	ORDER_ID,
	IRF_ID,
	STATUS,
	SURGEON_ID,
	SURGEON_TITLE,
	SURGEON_LAST_NAME,
	SURGEON_FIRST_NAME,
	PATIENT_FIRST_NAME,
	PATIENT_LAST_NAME,
	PATIENT_DOB,
	HOSPITAL_NAME,
	SURGERY_LOCATION,
	NVL(SALESREP_NUMBER, 'X') AS SALESREP_NUMBER,
	SALES_REP_NAME,
	CATALOG_NUMBER,
	UNIT_SELLING_PRICE,
	A.SERIAL_NUMBER,
	PRODUCT_CATEGORY,
	UDI_CATALOG,
	case 
		WHEN ( product_category LIKE 'iTOTAL CR%' OR product_category LIKE 'iTOTAL PS%' ) AND order_details_ordcnf IS NOT NULL THEN order_details_ordcnf
		ELSE product_description
	END AS PRODUCT_DESCRIPTION,
	product_desc_ordcnf,
	order_details_ordcnf,
	order_details2,
	CASE WHEN b.serial_number IS NULL THEN 0 ELSE 1 END AS iview_exists,
	CASE WHEN c.serial_number IS NULL THEN 0 ELSE 1 END AS dof_exists,
	CASE WHEN d.serial_number IS NULL THEN 0 ELSE 1 END AS coerdera_iview_exists,
	implant_request_date,
	sched_surgery_date,
	scan_received_date,
	commitment_date,
	shipped_date,
	rescheduled_surgery_date AS actual_surgery_date,
	rescheduled_surgery_date,
	line_id,
	line_type,
	currency,
	patient_gender,
	scan_cp_flag,
	region_code,
	territory_code,
	country_code,
	patella_flag
FROM
		xxcmis.xxcmis_conf_ord a
	LEFT OUTER JOIN (
		SELECT
			replace(replace(serial_number, 'A', ''), 'B', '') AS serial_number,
			serial_number AS serial_number_orig,
			entered_date
		FROM
			conforders.iview_serial_number
        ) b ON a.serial_number = b.serial_number
	LEFT OUTER JOIN conforders.dof_serial_number c ON a.serial_number = c.serial_number
	LEFT OUTER JOIN conforders.cordera_iview_serial_number d ON a.serial_number = c.serial_number
;