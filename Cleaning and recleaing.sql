-----------------------------------------------------------------------------------
-----------------------Cleaning and Recleaing data --------------------------------
-----------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-------------------Cleaning DATA for BAD_STAGE_REPORTED_CRIME--------------------------
---------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRO_CLEANING_BAD_STAGE_REPORTED_CRIME IS
CURSOR CURSOR_BAD_STAGE_REPORTED_CRIME IS
SELECT * FROM BAD_STAGE_REPORTED_CRIME ;

BEGIN
    FOR i IN CURSOR_BAD_STAGE_REPORTED_CRIME LOOP
        IF(UPPER(i.CRIME_STATUS) = 'OPEN' AND (i.CRIME_CLOSE_DATE IS NOT NULL OR i.CRIME_CLOSE_DATE IS NULL))
           THEN
              UPDATE BAD_STAGE_REPORTED_CRIME 
              SET CRIME_CLOSE_DATE = '01/01/9999'
              WHERE i.CRIME_ID = CRIME_ID;
        END IF;
          
        IF(UPPER(i.CRIME_STATUS) = 'ESCALATE' AND i.CRIME_CLOSE_DATE IS NOT NULL OR i.CRIME_CLOSE_DATE IS NULL) 
           THEN
              UPDATE BAD_STAGE_REPORTED_CRIME 
              SET CRIME_CLOSE_DATE = '01/01/9999'
              WHERE i.CRIME_ID = CRIME_ID;
        END IF;

        IF( i.CRIME_CLOSE_DATE IS NULL)
          THEN
              UPDATE BAD_STAGE_REPORTED_CRIME 
              SET CRIME_CLOSE_DATE = '01/01/9999'
              WHERE i.CRIME_ID = CRIME_ID;
        END IF;

        IF(i.CRIME_CLOSE_DATE > SYSDATE)
           THEN
              UPDATE BAD_STAGE_REPORTED_CRIME 
              SET CRIME_CLOSE_DATE = '01/01/9999'
              WHERE i.CRIME_ID = CRIME_ID;
        END IF;

        IF(i.DATE_REPORTED > SYSDATE)
           THEN
              UPDATE BAD_STAGE_REPORTED_CRIME 
              SET DATE_REPORTED = '01/01/9999'
              WHERE i.CRIME_ID = CRIME_ID;
        END IF;

        IF (UPPER(i.CRIME_STATUS) = 'CLOSED' AND i.CRIME_CLOSE_DATE < i.DATE_REPORTED)
             THEN
               UPDATE BAD_STAGE_REPORTED_CRIME 
               SET DATE_REPORTED = i.CRIME_CLOSE_DATE,
               CRIME_CLOSE_DATE = i.DATE_REPORTED
               WHERE i.CRIME_ID = CRIME_ID;
        END IF;

        IF(i.CRIME_CATEGORY is null)
              THEN
                UPDATE BAD_STAGE_REPORTED_CRIME 
                SET CRIME_CATEGORY = 'UNDEFINED'
                WHERE i.CRIME_ID = CRIME_ID;
        END IF;

        IF(i.FK1_OFFICER_ID is null)
             THEN
               UPDATE BAD_STAGE_REPORTED_CRIME 
               SET FK1_OFFICER_ID = -1
               WHERE i.CRIME_ID = CRIME_ID;
        END IF;
            

        IF(i.FK2_STATION_ID is null)
             THEN
               UPDATE BAD_STAGE_REPORTED_CRIME 
               SET FK2_STATION_ID = -1
               WHERE i.CRIME_ID = CRIME_ID;
        END IF;
          
          -- RESETTING ERROR STATUS  TO FIXED AFTER DATA IS BEING CLEANED 
        UPDATE ISSUE_LOG
        SET ISSUE_STATUS = 'FIXED', DATE_FIXED = SYSDATE
        WHERE TABLE_KEY = i.CRIME_ID 
        AND LOWER(ISSUE_TABLE) = 'stage_reported_crime';
            
          -- UPDATING ALL VARCHAR TYPE VALUE TO UPPER CASE MAKING IT STANDARD CASE
        UPDATE BAD_STAGE_REPORTED_CRIME 
        SET CRIME_CATEGORY = UPPER(CRIME_CATEGORY),CRIME_STATUS = UPPER(CRIME_STATUS)
        WHERE i.CRIME_ID = CRIME_ID;


    END LOOP;
END;
/

BEGIN
PRO_CLEANING_BAD_STAGE_REPORTED_CRIME;
END;
/

----------------------------------------------------------------------------------------
-------------------ReCleaning DATA for Cleaning DATA for BAD_STAGE_REPORTED_CRIME-------
----------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRO_RE_CLEANING_BAD_STAGE_REPORTED_CRIME IS
CURSOR CURSOR_RE_BAD_STAGE_REPORTED_CRIME IS
SELECT * FROM BAD_STAGE_REPORTED_CRIME;

BEGIN
  FOR i IN  CURSOR_RE_BAD_STAGE_REPORTED_CRIME LOOP
    IF(i.DATE_REPORTED = '01/01/9999' AND i.DATASOURCE = 'PRCS')
      THEN 
      UPDATE BAD_STAGE_REPORTED_CRIME
      SET DATE_REPORTED = (SELECT DATE_REPORTED FROM pl_reported_crime WHERE DATE_REPORTED < SYSDATE AND REPORTED_CRIME_ID = i.PK_REPORT_ID)
      WHERE i.CRIME_ID = CRIME_ID;
    END IF;

    IF(i.DATE_REPORTED = '01/01/9999' AND i.DATASOURCE = 'WALE')
      THEN 
        UPDATE BAD_STAGE_REPORTED_CRIME
        SET  DATE_REPORTED = (SELECT reported_date  FROM CRIME_REGISTER  WHERE reported_datE IS NOT NULL AND reported_date < SYSDATE AND crime_id = i.PK_REPORT_ID)
        WHERE i.CRIME_ID = CRIME_ID;
    END IF;

    -- RESETTING ERROR STATUS  TO FIXED AFTER DATA IS BEING CLEANED 
        UPDATE ISSUE_LOG
        SET ISSUE_STATUS = 'FIXED', DATE_FIXED = SYSDATE
        WHERE TABLE_KEY = i.CRIME_ID 
        AND LOWER(ISSUE_TABLE) = 'stage_reported_crime';

  END LOOP;
END;
/

BEGIN
PRO_RE_CLEANING_BAD_STAGE_REPORTED_CRIME;
END;
/

---------------------------------------------------------------------------------------------------------------------------------------------
-------------------Merging bad table's data( BAD_STAGE_REPORTED_CRIME) into good table(GOOD_STAGE_REPORTED_CRIME) ---------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

MERGE INTO GOOD_STAGE_REPORTED_CRIME GD
USING BAD_STAGE_REPORTED_CRIME BD
ON(GD.CRIME_ID = BD.CRIME_ID)
     
     WHEN NOT MATCHED THEN
       INSERT(CRIME_ID, PK_REPORT_ID, DATE_REPORTED, CRIME_CATEGORY, CRIME_STATUS, CRIME_CLOSE_DATE, FK1_OFFICER_ID, FK2_STATION_ID, DATASOURCE)
       VALUES(BD.CRIME_ID, BD.PK_REPORT_ID ,BD.DATE_REPORTED, BD.CRIME_CATEGORY, BD.CRIME_STATUS, BD.CRIME_CLOSE_DATE, BD.FK1_OFFICER_ID, BD.FK2_STATION_ID, BD.DATASOURCE);

----------------------------------------------------------------------------------------
-------------------Cleaning DATA for BAD_Police_employee--------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRO_CLEANING_BAD_Police_employee IS
CURSOR CUESOR_BAD_Police_employee IS
SELECT * FROM BAD_Police_employee;

BEGIN
  FOR i IN CUESOR_BAD_Police_employee  LOOP
    IF(i.FK1_STATION_ID IS NULL)
      THEN
       UPDATE BAD_Police_employee 
       SET FK1_STATION_ID = -1
       WHERE i.OFFICER_ID = OFFICER_ID;
    END IF;

     -- RESETTING ERROR STATUS  TO FIXED AFTER DATA IS BEING CLEANED 
        UPDATE ISSUE_LOG
        SET ISSUE_STATUS = 'FIXED', DATE_FIXED = SYSDATE
        WHERE TABLE_KEY = i.OFFICER_ID
        AND LOWER(ISSUE_TABLE) = 'police_employee';

  END LOOP;
END;
/

BEGIN
PRO_CLEANING_BAD_Police_employee;
END;
/


----------------------------------------------------------------------------------------
-------------------ReCleaning DATA for BAD_Police_employee------------------------------
----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PRO_RECLEANING_BAD_Police_employee IS
CURSOR CUESOR_RE_BAD_Police_employee IS
SELECT * FROM BAD_Police_employee;
BEGIN
  FOR i IN CUESOR_RE_BAD_Police_employee LOOP
    IF(i.LASTNAME IS NULL AND i.DATASOURCE = 'PRCS')
      THEN
        UPDATE BAD_Police_employee
        SET LASTNAME = (SELECT SUBSTR(emp_name, INSTR(emp_name, ' ')+1) FROM pl_police_employee WHERE emp_id = i.PK_POLICE_ID)
       WHERE i.OFFICER_ID = OFFICER_ID;

    ELSIF(i.LASTNAME IS NULL AND i.DATASOURCE = 'WALE')
      THEN
      UPDATE BAD_Police_employee
      SET LASTNAME = (SELECT middle_name ||' '|| last_name FROM OFFICER WHERE officer_id  = i.PK_POLICE_ID)
      WHERE i.OFFICER_ID = OFFICER_ID;

    ELSIF(REGEXP_LIKE(i.firstname, '[#!$^&*%./\|]') AND i.DATASOURCE = 'PRCS')
    THEN 
      UPDATE BAD_Police_employee
      SET FIRSTNAME = (SELECT SUBSTR(emp_name,1, INSTR(emp_name, ' ')-1) FROM pl_police_employee WHERE emp_id =  i.PK_POLICE_ID)
      WHERE i.OFFICER_ID = OFFICER_ID;
        
    ELSIF(REGEXP_LIKE(i.firstname, '[#!$^&*%./\|]') AND i.DATASOURCE = 'WALE')    
      THEN
        UPDATE BAD_Police_employee
        SET FIRSTNAME= (SELECT first_name FROM OFFICER WHERE officer_id =  i.PK_POLICE_ID)
        WHERE i.OFFICER_ID = OFFICER_ID;
    ELSE
      -- DO NOTHING
      UPDATE BAD_Police_employee
      SET FIRSTNAME= i.FIRSTNAME
      WHERE i.OFFICER_ID = OFFICER_ID;
    END IF;

 -- RESETTING ERROR STATUS  TO FIXED AFTER DATA IS BEING CLEANED 
      UPDATE ISSUE_LOG
        SET ISSUE_STATUS = 'FIXED', DATE_FIXED = SYSDATE
        WHERE TABLE_KEY = i.OFFICER_ID
        AND LOWER(ISSUE_TABLE) = 'police_employee';
  END LOOP;
END;
/

BEGIN
PRO_RECLEANING_BAD_Police_employee ;
END;
/

---------------------------------------------------------------------------------------------------------------------------------------------
-------------------Merging bad table's data( BAD_Police_employee) into good table(GOOD_Police_employee) ---------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

MERGE INTO GOOD_Police_employee GP
USING BAD_Police_employee BP
ON(BP.OFFICER_ID = GP.OFFICER_ID)
    

        WHEN NOT MATCHED THEN
           INSERT(OFFICER_ID,PK_POLICE_ID,FIRSTNAME,LASTNAME,FK1_STATION_ID,DATASOURCE)
           VALUES(BP.OFFICER_ID, BP.PK_POLICE_ID, BP.FIRSTNAME, BP.LASTNAME, BP.FK1_STATION_ID, BP.DATASOURCE);

----------------------------------------------------------------------------------------
-------------------Cleaning DATA for BAD_Station----------------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRO_CLEANING_BAD_Station IS
CURSOR CUESOR_BAD_Station IS
SELECT * FROM BAD_Station;
BEGIN
  FOR i IN CUESOR_BAD_Station LOOP
    IF(REGEXP_LIKE(i.STATION_NAME, '[#!$^&*%./\|]' ) AND i.DATASOURCE = 'PRCS')
      THEN
        UPDATE BAD_Station
        SET STATION_NAME = (SELECT station_name FROM pl_station WHERE station_id = i.PK_STATION_ID)
        WHERE STATION_NO = i.STATION_NO;

    ELSIF(REGEXP_LIKE(i.STATION_NAME, '[#!$^&*%./\|]' ) AND i.DATASOURCE = 'WALE')    
      THEN
        UPDATE BAD_Station
        SET STATION_NAME = (SELECT city_name FROM LOCATION WHERE location_id = i.PK_STATION_ID)
        WHERE STATION_NO = i.STATION_NO;

    ELSIF(REGEXP_LIKE(i.REGION, '[#!$^&*%./\|]' ) OR i.REGION is null AND i.DATASOURCE = 'PRCS')    
      THEN
        UPDATE BAD_Station
        SET REGION = (SELECT PA.area_name FROM pl_station PL, pl_area PA WHERE PL.fk1_area_id = PA.area_id AND i.PK_STATION_ID = station_id)
        WHERE STATION_NO = i.STATION_NO; 

    ELSIF(REGEXP_LIKE(i.REGION, '[#!$^&*%./\|]' ) OR i.REGION is null AND i.DATASOURCE = 'WALE')    
      THEN
        UPDATE BAD_Station
        SET REGION  = (SELECT region_name FROM LOCATION L ,REGION R WHERE  L.region_id = R.region_id AND i.PK_STATION_ID = L.location_id)
        WHERE STATION_NO = i.STATION_NO;     
    END IF;

-- RESETTING ERROR STATUS  TO FIXED AFTER DATA IS BEING CLEANED 
        UPDATE ISSUE_LOG
        SET ISSUE_STATUS = 'FIXED', DATE_FIXED = SYSDATE
        WHERE TABLE_KEY = i.STATION_NO
        AND LOWER(ISSUE_TABLE) = 'station';


  END LOOP;
END;
/
-- No need to recleaning  NEEDED

BEGIN
PRO_CLEANING_BAD_Station ;
END;
/



-------------------------------------------------------------------------------------------------------------------
-------------------Merging bad table's data( BAD_Station) into good table(GOOD_Station) ---------------------------
-------------------------------------------------------------------------------------------------------------------
MERGE INTO GOOD_STATION GS
USING BAD_STATION BS
ON (BS.STATION_NO = GS.STATION_NO)
   WHEN  MATCHED THEN
      UPDATE SET 
       GS.PK_STATION_ID	= BS.PK_STATION_ID,
       GS.STATION_NAME	= BS.STATION_NAME,
       GS.REGION = BS.REGION,
       GS.DATASOURCE = BS.DATASOURCE
       WHERE BS.STATION_NO = GS.STATION_NO
   WHEN NOT MATCHED THEN
       INSERT (STATION_NO,PK_STATION_ID,STATION_NAME,REGION,DATASOURCE)   
       VALUES (BS.STATION_NO,BS.PK_STATION_ID,BS.STATION_NAME,BS.REGION,BS.DATASOURCE);
   
        

----------------------------------------------------------------------------------------
-------------------Cleaning DATA for BAD_Crime_details----------------------------------
----------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE PRO_CLEANING_BAD_Crime_details IS
CURSOR CUESOR_BAD_Crime_details IS
SELECT * FROM BAD_Crime_details;

BEGIN 
  FOR i IN CUESOR_BAD_Crime_details LOOP
    IF(i.WORK_START_DATE IS NULL OR i.WORK_START_DATE > SYSDATE OR i.WORK_START_DATE > i.WORK_END_DATE)
    THEN 
      UPDATE BAD_Crime_details
      SET 
      WORK_START_DATE = '01/01/9999'
      WHERE C_DETAIL_ID = i.C_DETAIL_ID;
    END IF;

    IF(i.WORK_END_DATE IS NULL OR i.WORK_END_DATE > SYSDATE)
    THEN
      UPDATE BAD_Crime_details
      SET
      WORK_END_DATE = '01/01/9999'
      WHERE C_DETAIL_ID = i.C_DETAIL_ID;
    END IF;

    -- RESETTING ERROR STATUS  TO FIXED AFTER DATA IS BEING CLEANED 
        UPDATE ISSUE_LOG
        SET ISSUE_STATUS = 'FIXED', DATE_FIXED = SYSDATE
        WHERE TABLE_KEY = i.C_DETAIL_ID 
        AND LOWER(ISSUE_TABLE) = 'crime_details';

  END LOOP;
END;
/

BEGIN
PRO_CLEANING_BAD_Crime_details;
END;
/


----------------------------------------------------------------------------------------
-------------------ReCleaning DATA for BAD_Crime_details--------------------------------
----------------------------------------------------------------------------------------

 CREATE OR REPLACE PROCEDURE PRO_RECLEANING_BAD_Crime_details IS
CURSOR CUESOR_RC_BAD_Crime_details IS
SELECT * FROM BAD_Crime_details;

BEGIN
  FOR i IN CUESOR_RC_BAD_Crime_details LOOP
    IF(i.DATASOURCE = 'PRCS') THEN
         UPDATE BAD_Crime_details
         SET WORK_START_DATE =  (SELECT work_start_date
                              FROM pl_work_allocation 
                              WHERE work_start_date < SYSDATE
                              AND work_start_date IS NOT NULL
                              AND work_start_date < work_end_date
                              AND S_REPORTED_CRIME_ID = i.CRIME_ID
                              AND D_EMP_ID = i.OFFICER_ID 
                               ) WHERE C_DETAIL_ID =i.C_DETAIL_ID; 
            UPDATE BAD_Crime_details
           set  WORK_END_DATE   = (SELECT work_end_date 
                              FROM pl_work_allocation
                              WHERE work_end_date < SYSDATE
                              AND work_end_date IS NOT NULL
                              AND S_REPORTED_CRIME_ID = i.CRIME_ID
                              AND D_EMP_ID= i.OFFICER_ID)
             WHERE C_DETAIL_ID = i.C_DETAIL_ID;                 

    END IF;

        -- RESETTING ERROR STATUS  TO FIXED AFTER DATA IS BEING CLEANED 
        UPDATE ISSUE_LOG
        SET ISSUE_STATUS = 'FIXED', DATE_FIXED = SYSDATE
        WHERE TABLE_KEY = i.C_DETAIL_ID 
        AND LOWER(ISSUE_TABLE) = 'crime_details';
        
  END LOOP;
END;
/

BEGIN
PRO_RECLEANING_BAD_Crime_details;
END;
/

------------------------------------------------------------------------------------------------------------------
-------------------Merging bad table's data( BAD_Crime_details) into good table(GOOD_Crime_details) --------------
------------------------------------------------------------------------------------------------------------------

MERGE INTO GOOD_Crime_details GC
USING BAD_Crime_details BC
ON(GC.C_DETAIL_ID = BC.C_DETAIL_ID)
 

           WHEN NOT MATCHED THEN
                INSERT(C_DETAIL_ID,CRIME_ID,OFFICER_ID,WORK_START_DATE,WORK_END_DATE,DATASOURCE)
                VALUES(BC.C_DETAIL_ID,BC.CRIME_ID,BC.OFFICER_ID,BC.WORK_START_DATE,BC.WORK_END_DATE,BC.DATASOURCE);

------------------------------------------------------------------------------------------------------------------               