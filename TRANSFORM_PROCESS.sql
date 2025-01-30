-------------------------------------------------------------------------------------------------
--------------TRNSFERRING ALL DATA FROM GOOD TABLES TO TRANSFER TABLE ---------------------------
-------------------------------------------------------------------------------------------------


 -------------------------------------------------------------------------------
--------------CREATING TRANSFER_REPORTED_CRIME TABLE ---------------------------
--------------------------------------------------------------------------------
DROP TABLE TRANSFER_REPORTED_CRIME ;
 CREATE TABLE TRANSFER_REPORTED_CRIME 
 AS SELECT * FROM GOOD_STAGE_REPORTED_CRIME;
 DELETE TRANSFER_REPORTED_CRIME ;



-------------------------------------------------------------------------------------------------------------------------------
--------------TRNSFERRING ALL DATA FROM GOOD_STAGE_REPORTED_CRIMES TO TRANSFER_REPORTED_CRIME TABLE ---------------------------
-------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PRO_TRANSFER_REPORTED_CRIME_TRANS IS
BEGIN
        MERGE INTO TRANSFER_REPORTED_CRIME TR
        USING GOOD_STAGE_REPORTED_CRIME GR
        ON (TR.CRIME_ID = GR.CRIME_ID)
                WHEN NOT MATCHED THEN
                     INSERT(CRIME_ID,PK_REPORT_ID,DATE_REPORTED,CRIME_CATEGORY,CRIME_STATUS,CRIME_CLOSE_DATE,FK1_OFFICER_ID,FK2_STATION_ID,DATASOURCE)
                     VALUES(GR.CRIME_ID,GR.PK_REPORT_ID,GR.DATE_REPORTED,GR.CRIME_CATEGORY,GR.CRIME_STATUS,GR.CRIME_CLOSE_DATE,GR.FK1_OFFICER_ID,GR.FK2_STATION_ID,GR.DATASOURCE);
END;
/

BEGIN
PRO_TRANSFER_REPORTED_CRIME_TRANS;
END;
/
 -----------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE CLEANING IS
CURSOR CURSOR_C IS
SELECT * FROM TRANSFER_REPORTED_CRIME;

BEGIN
  FOR i IN CURSOR_C  LOOP
    IF(i.CRIME_CLOSE_DATE is NULL)
      THEN
        UPDATE TRANSFER_REPORTED_CRIME
        SET CRIME_CLOSE_DATE = '01/01/9999'
        WHERE CRIME_ID = i.CRIME_ID;
    END IF;
  END LOOP;
END;
/

BEGIN
CLEANING;
END;
/

 -------------------------------------------------------------------------------------------------------------------------------------
--------------MAKING STANDRAD VARCHAR VALUE OF  TRANSFER_REPORTED_CRIME; TABLE TO AVOID CASE RELATED ISSUES --------------------------
--------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PRO_STANDARD_TRANSFER_REPORTED_CRIME IS
CURSOR CURSOR_STANDARD_TRANSFER_REPORTED_CRIME IS
SELECT * FROM TRANSFER_REPORTED_CRIME;

BEGIN
  FOR i IN CURSOR_STANDARD_TRANSFER_REPORTED_CRIME LOOP
    UPDATE TRANSFER_REPORTED_CRIME
    SET CRIME_CATEGORY = UPPER(i.CRIME_CATEGORY),
    	CRIME_STATUS = UPPER(i.CRIME_STATUS)
        WHERE  CRIME_ID = i.CRIME_ID;
  END LOOP;
END;
/

BEGIN
PRO_STANDARD_TRANSFER_REPORTED_CRIME;
END;
/
-------------------------------------------------------------------------------------------------------------------------------
--------------GENERALIZING CRIME CATEGORY IN TRANSFER_REPORTED_CRIME TABLE ---------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------

UPDATE TRANSFER_REPORTED_CRIME 
SET CRIME_CATEGORY = UPPER('THEFT')
WHERE CRIME_CATEGORY IN ('CAR THEFT','ROBBERY','ARMED ROBBERY');

UPDATE TRANSFER_REPORTED_CRIME 
SET CRIME_CATEGORY = UPPER('VIOLENT CRIME ')
WHERE CRIME_CATEGORY ='HIT AND RUN';


UPDATE TRANSFER_REPORTED_CRIME
SET FK2_STATION_ID = -1
WHERE FK2_STATION_ID = 0;
 -------------------------------------------------------------------------------
--------------CREATING TRANSFER_Police_employee TABLE --------------------------
--------------------------------------------------------------------------------
DROP TABLE TRANSFER_POLICE_EMPLOYEE;
CREATE TABLE TRANSFER_POLICE_EMPLOYEE AS SELECT * FROM GOOD_POLICE_EMPLOYEE;
DELETE TRANSFER_POLICE_EMPLOYEE;



-------------------------------------------------------------------------------------------------------------------------------
--------------TRNSFERRING ALL DATA FROM GOOD_POLICE_EMPLOYEE TO TRANSFER_POLICE_EMPLOYEE -------------------------------------
-------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRO_TRANSFER_POLICE_EMPLOYEE_TRANS IS
BEGIN
        MERGE INTO TRANSFER_POLICE_EMPLOYEE TE
        USING GOOD_Police_employee GE
        ON(TE.OFFICER_ID = GE.OFFICER_ID)
                 WHEN NOT MATCHED THEN
                 INSERT(OFFICER_ID,PK_POLICE_ID,FIRSTNAME,LASTNAME,FK1_STATION_ID,DATASOURCE)
                 VALUES(GE.OFFICER_ID,GE.PK_POLICE_ID,GE.FIRSTNAME,GE.LASTNAME,GE.FK1_STATION_ID,GE.DATASOURCE);
END;
/

BEGIN
PRO_TRANSFER_POLICE_EMPLOYEE_TRANS;
END;
/

 -------------------------------------------------------------------------------------------------------------------------------------
--------------MAKING STANDRAD VARCHAR VALUE OF  TRANSFER_POLICE_EMPLOYEE TABLE TO AVOID CASE RELATED ISSUES --------------------------
--------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PRO_STANDARD_TRANSFER_POLICE_EMPLOYEE IS
CURSOR CURSOR_STANDARD_TRANSFER_POLICE_EMPLOYEE IS
SELECT * FROM TRANSFER_POLICE_EMPLOYEE;

BEGIN
  FOR i IN CURSOR_STANDARD_TRANSFER_POLICE_EMPLOYEE LOOP
    UPDATE TRANSFER_POLICE_EMPLOYEE
    SET FIRSTNAME = UPPER(i.FIRSTNAME),
    	LASTNAME = UPPER(i.LASTNAME)
        WHERE  OFFICER_ID = i.OFFICER_ID;
  END LOOP;
END;
/

BEGIN
PRO_STANDARD_TRANSFER_POLICE_EMPLOYEE;
END;
/


 -------------------------------------------------------------------------------
--------------CREATING TRANSFER_STATION TABLE ---------------------------------
--------------------------------------------------------------------------------
DROP TABLE TRANSFER_STATION;
CREATE TABLE TRANSFER_STATION AS SELECT * FROM GOOD_Station;
DELETE TRANSFER_STATION;


-------------------------------------------------------------------------------------------------------------------------------
--------------TRNSFERRING ALL DATA FROM GOOD_STATIONTO TRANSFER_STATION ------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PRO_TRANSFER_STATION_TRANS IS
BEGIN
        MERGE INTO TRANSFER_STATION T
        USING GOOD_STATION G
        ON(T.STATION_NO = T.STATION_NO)
                     WHEN NOT MATCHED THEN
                     INSERT(STATION_NO,PK_STATION_ID,STATION_NAME,REGION,DATASOURCE)
                     VALUES(G.STATION_NO,G.PK_STATION_ID,G.STATION_NAME,G.REGION,G.DATASOURCE);

END;
/

BEGIN
PRO_TRANSFER_STATION_TRANS;
END;
/


 -------------------------------------------------------------------------------------------------------------------------------------
--------------MAKING STANDRAD VARCHAR VALUE OF  TRANSFER_STATION TABLE TO AVOID CASE RELATED ISSUES --------------------------
--------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PRO_STANDARD_TRANSFER_STATION IS
CURSOR CURSOR_STANDARD_TRANSFER_STATION IS
SELECT * FROM TRANSFER_STATION;

BEGIN
  FOR i IN CURSOR_STANDARD_TRANSFER_STATION LOOP
    UPDATE TRANSFER_STATION
    SET STATION_NAME = UPPER(i.STATION_NAME),
    	REGION= UPPER(i.REGION)
        WHERE  STATION_NO = i.STATION_NO;
  END LOOP;
END;
/

BEGIN
PRO_STANDARD_TRANSFER_STATION;
END;
/
---------------------------------------------
---------------------------------------------
INSERT INTO TRANSFER_STATION(STATION_NO,PK_STATION_ID,STATION_NAME,REGION,DATASOURCE)
VALUES(-1,-1,'UNDEFINED','UNDEFINED','UNDEFINED');

 -------------------------------------------------------------------------------
--------------CREATING TRANSFER_Crime_details ---------------------------------
--------------------------------------------------------------------------------
DROP TABLE TRANSFER_CRIME_DETAILS;
CREATE TABLE TRANSFER_CRIME_DETAILS AS SELECT * FROM GOOD_Crime_details;
DELETE  TRANSFER_CRIME_DETAILS;



-------------------------------------------------------------------------------------------------------------------------------
--------------TRNSFERRING ALL DATA FROM GOOD_Crime_details TO TRANSFER_CRIME_DETAILS ------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PRO_TRANSFER_CRIME_DETAILS_TRANS IS
BEGIN
        MERGE INTO TRANSFER_CRIME_DETAILS TC
        USING GOOD_Crime_details GD
        ON(TC.C_DETAIL_ID = GD.C_DETAIL_ID)
                 WHEN NOT MATCHED THEN
                 INSERT(C_DETAIL_ID,CRIME_ID,OFFICER_ID,WORK_START_DATE,WORK_END_DATE,DATASOURCE)
                 VALUES(GD.C_DETAIL_ID,GD.CRIME_ID,GD.OFFICER_ID,GD.WORK_START_DATE,GD.WORK_END_DATE,GD.DATASOURCE);
END;      
/

BEGIN
PRO_TRANSFER_CRIME_DETAILS_TRANS;
END;
/


-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------



