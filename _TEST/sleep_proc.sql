--#SET TERMINATOR @
-- Copied from https://www.ibm.com/developerworks/community/blogs/SQLTips4DB2LUW/entry/sleep?lang=en
CREATE OR REPLACE PROCEDURE SLEEP_PROC(seconds INTEGER) 
BEGIN
  DECLARE end TIMESTAMP;
  SET end = CURRENT TIMESTAMP + seconds SECONDS;
wait:  LOOP
    IF CURRENT TIMESTAMP >= end THEN LEAVE wait; END IF;
  END LOOP wait;
END
@
--#SET TERMINATOR ;
