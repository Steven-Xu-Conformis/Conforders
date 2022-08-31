create or replace trigger CONFORDERS.TR_CO_UPDATE_IVIEW_SN
before insert or update on CONFORDERS.IVIEW_SERIAL_NUMBER
for each row
--when (old.SERIAL_NUMBER is null or new.SERIAL_NUMBER != old.SERIAL_NUMBER)

begin
    :new.SERIAL_NUMBER_ORI := REGEXP_REPLACE(:new.SERIAL_NUMBER,'^(.+)[AB\-].*','\1');
end;
