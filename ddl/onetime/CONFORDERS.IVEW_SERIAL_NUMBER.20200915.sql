alter table CONFORDERS.IVIEW_SERIAL_NUMBER add SERIAL_NUMBER_ORI as (regexp_replace(SERIAL_NUMBER,'^(.+)[AB\-].*','\1'));
create index CONFORDERS.IX_SERIAL_NUMBER_ORI on CONFORDERS.IVIEW_SERIAL_NUMBER (SERIAL_NUMBER_ORI);