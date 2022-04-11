create or replace view APPS.XXCMIS_SURGEON_HOSPITAL_V as
select 
	roles.CUST_ACCOUNT_ROLE_ID,
	phys.PARTY_ID as SURGEON_ID, 
	phys.PERSON_FIRST_NAME as FIRSTNAME,
	phys.PERSON_MIDDLE_NAME as MIDDLENAME,
	phys.PERSON_LAST_NAME as LASTNAME, 
	'(' || phone.PHONE_AREA_CODE || ')' || phone.PHONE_NUMBER || ' ' || phone.PHONE_EXTENSION as PHONE,
	email.EMAIL_ADDRESS as EMAIL,
	loc.ADDRESS1,
	loc.ADDRESS2,
	loc.ADDRESS3,
	loc.CITY,
	loc.STATE,
	loc.POSTAL_CODE as POSTALCODE,
	loc.COUNTRY,
	hosp.PARTY_NAME as HOSPITAL,
	hosp.PARTY_ID as HOSPITAL_ID,
	rel.STATUS as ACTIVE,
	roles.CURRENT_ROLE_STATE as PHYS_STATUS,
    rel.START_DATE as REL_START_DATE,
    rel.END_DATE as REL_END_DATE,
    rel.STATUS as REL_STATUS,
    org_cont.STATUS as ORG_CONTACT_STATUS,
    sites.STATUS as ACCOUNT_SITE_STATUS,
    party_sites.STATUS as PARTY_SITE_STATUS
from
	APPS.HZ_CUST_ACCOUNT_ROLES roles 
	left outer join APPS.HZ_RELATIONSHIPS rel on roles.PARTY_ID = rel.PARTY_ID and rel.RELATIONSHIP_CODE = 'CONTACT'
	join APPS.HZ_ORG_CONTACTS org_cont on org_cont.PARTY_RELATIONSHIP_ID = rel.RELATIONSHIP_ID and UPPER (org_cont.job_title) = 'PHYSICIAN'
	left outer join APPS.HZ_PARTIES phys on phys.PARTY_ID = rel.OBJECT_ID and  rel.OBJECT_TABLE_NAME = 'HZ_PARTIES' and rel.OBJECT_TYPE = 'PERSON'
	left outer join APPS.HZ_PARTIES hosp on hosp.PARTY_ID = rel.SUBJECT_ID and rel.SUBJECT_TABLE_NAME = 'HZ_PARTIES' and rel.SUBJECT_TYPE = 'ORGANIZATION'
	left outer join HZ_CUST_ACCT_SITES_ALL sites on roles.CUST_ACCOUNT_ID = sites.CUST_ACCOUNT_ID and roles.CUST_ACCT_SITE_ID = sites.CUST_ACCT_SITE_ID
	left outer join HZ_PARTY_SITES party_sites on party_sites.PARTY_SITE_ID = sites.PARTY_SITE_ID
	left outer join HZ_LOCATIONS loc on loc.LOCATION_ID = party_sites.LOCATION_ID
	left outer join APPS.HZ_CONTACT_POINTS phone on rel.PARTY_ID = phone.OWNER_TABLE_ID and phone.CONTACT_POINT_TYPE = 'PHONE' and phone.OWNER_TABLE_NAME = 'HZ_PARTIES' and phone.STATUS = 'A' and phone.PRIMARY_FLAG = 'Y'
	left outer join APPS.HZ_CONTACT_POINTS email on rel.PARTY_ID = email.OWNER_TABLE_ID and email.CONTACT_POINT_TYPE = 'EMAIL' and email.OWNER_TABLE_NAME = 'HZ_PARTIES' and email.STATUS = 'A' and email.PRIMARY_FLAG = 'Y'
;