---------------------------------------------------------------------------------------------
---------------MESSING WITH DATA BEFORE SEPERATION (GOOD AND BAD DATA)------------------------
---------------------------------------------------------------------------------------------
 ----TABALE stage_reported_crime------
CREATE OR REPLACE PROCEDURE PR_ALTERING_DATA IS
BEGIN
 ----TABALE stage_reported_crime------
UPDATE stage_reported_crime    -- Adding close date while case is still open
SET crime_close_date = SYSDATE 
WHERE UPPER(crime_status) = 'OPEN'
AND PK_REPORT_ID IN (3,5,56,440);

UPDATE stage_reported_crime  -- Altering crime reported date
SET date_reported ='05/12/2033'
WHERE UPPER(crime_status) = 'CLOSED'
AND PK_REPORT_ID IN (2,65,240,28,14,6);

-- UPDATE stage_reported_crime
-- SET crime_status =UPPER('OPEN')
-- WHERE PK_REPORT_ID IN (22,24,29);

 ----TABALE Police_employee------
 UPDATE Police_employee  -- SETTING LASTNAME NULL
 SET lastname = NULL
 WHERE firstname IN ('Ashley','Drew');

  UPDATE Police_employee 
 SET firstname = 'Cliff##Tucker'
 WHERE lastname IN ('Cliff Tucker');
 
   UPDATE Police_employee -- ADDING SPECIAL CHARACTER IN FIRSTNAME
 SET firstname = '@64863*$'
 where PK_POLICE_ID = 13;

 ----TABALE Station------
  UPDATE Station 
  SET region = '@MEANWOOD'
  WHERE station_name = 'MEANWOOD';

  UPDATE Station 
 	set REGION = NULL
     WHERE station_name ='Kings Cross';

    UPDATE Station   
    SET station_name= '##mid&W'
    WHERE REGION = 'Mid WALES';

     ----TABALE Crime_details------
   UPDATE Crime_details 
   SET WORK_START_DATE = '08/25/2056'
   WHERE CRIME_ID = 3;

END;
/

BEGIN
PR_ALTERING_DATA;
END;
/

----------------------------------------------------------------------------------------
-----CREATING GOOD AND BAD TABLE FOR EACH STAGING TABLE FOR DATA SEPERATION ------------
----------------------------------------------------------------------------------------

-----------GOOD TABLE FOR STAGE_REPORTED_CRIME ---------
DROP TABLE GOOD_STAGE_REPORTED_CRIME;
create table GOOD_STAGE_REPORTED_CRIME as select * from STAGE_REPORTED_CRIME;
DELETE GOOD_STAGE_REPORTED_CRIME;

-----------BAD TABLE FOR STAGE_REPORTED_CRIME ---------
DROP TABLE BAD_STAGE_REPORTED_CRIME;
create table BAD_STAGE_REPORTED_CRIME as select * from STAGE_REPORTED_CRIME;
DELETE BAD_STAGE_REPORTED_CRIME;

-----------GOOD TABLE FOR Police_employee---------
DROP TABLE GOOD_Police_employee;
create table GOOD_Police_employee as select * from Police_employee;
DELETE GOOD_Police_employee;

-----------BAD TABLE FOR Police_employee---------
DROP TABLE BAD_Police_employee;
create table BAD_Police_employee as select * from Police_employee;
DELETE BAD_Police_employee;

-----------GOOD TABLE FOR Station---------
DROP TABLE GOOD_Station;
create table GOOD_Station as select * from Station;
DELETE GOOD_Station;

-----------BAD TABLE FOR Station---------
DROP TABLE BAD_Station;
create table BAD_Station as select * from Station;
DELETE BAD_Station;

-----------GOOD TABLE Crime_details---------
DROP TABLE GOOD_Crime_details;
create table GOOD_Crime_details as select * from Crime_details;
DELETE GOOD_Crime_details;

-----------BAD TABLE Crime_details---------
DROP TABLE BAD_Crime_details;
create table BAD_Crime_details as select * from Crime_details;
DELETE BAD_Crime_details;


---------------------------------------------------------------------------------
------------ERROR_LOG TABLE(FOR ERROR DESCRIPTION)---------------------------
---------------------------------------------------------------------------------
 ------ ERROR_LOG FOR BAD_STAGE_REPORTED_CRIME ---
DROP TABLE issue_log;
CREATE TABLE issue_log(
        issue_id INTEGER NOT NULL,
        issue_desc VARCHAR(80) NOT NULL,
        issue_status VARCHAR(80) NOT NULL,
        issue_table VARCHAR(80) NOT NULL,
        table_key INTEGER NOT NULL,
        time TIMESTAMP NOT NULL,
        date_fixed TIMESTAMP ,
        constraint pk_issue_id PRIMARY KEY(issue_id)
);

DROP SEQUENCE issue_log_seq;
CREATE SEQUENCE issue_log_seq
  START WITH 1
  INCREMENT BY 1
  MINVALUE 1;
/

   CREATE OR REPLACE TRIGGER issue_log_TRIG
BEFORE INSERT ON issue_log
FOR EACH ROW 
BEGIN
    IF :NEW.issue_id IS NULL THEN
        SELECT issue_log_seq.NEXTVAL INTO :NEW.issue_id FROM SYS.DUAL;
    END IF;    
END;
/
---------------------------------------------------------------------------------
------------DATA SEPERATION (GOOD TABLE AND BAD TABLE)---------------------------
---------------------------------------------------------------------------------

--------- DATA SEPERATION FOR stage_reported_crime---------

CREATE OR REPLACE PROCEDURE PR_SEPERATION_stage_reported_crime IS
CURSOR CURSOR_SEPERATION_stage_reported_crime IS
   SELECT * FROM stage_reported_crime 
     WHERE NOT EXISTS(
    SELECT * FROM GOOD_STAGE_REPORTED_CRIME ,BAD_STAGE_REPORTED_CRIME
    WHERE stage_reported_crime.crime_id = GOOD_STAGE_REPORTED_CRIME.crime_id
  or stage_reported_crime.crime_id = BAD_STAGE_REPORTED_CRIME.crime_id
   );
 BEGIN
  FOR i IN CURSOR_SEPERATION_stage_reported_crime LOOP
   if (i.pk_report_id is not null 
                and i.date_reported is not null 
                and i.crime_category is not null 
                and i.crime_status is not null 
                and i.fk1_officer_id is not null 
                and i.fk2_station_id is not null 
                AND UPPER(i.crime_status) ='OPEN' 
                and i.crime_close_date is null  
                and i.date_reported < sysdate) 
                THEN
                     insert into GOOD_STAGE_REPORTED_CRIME (crime_id,pk_report_id,date_reported,crime_category,crime_status,crime_close_date,fk1_officer_id,fk2_station_id,datasource)
                     values(i.crime_id,i.pk_report_id,i.date_reported,i.crime_category,i.crime_status,i.crime_close_date,i.fk1_officer_id,i.fk2_station_id,i.datasource);
   elsif (i.pk_report_id is not null 
                and i.date_reported is not null
                and i.crime_category is not null 
                and i.crime_status is not null 
                and i.fk1_officer_id is not null 
                and i.fk2_station_id is not null 
                AND UPPER(i.crime_status) ='CLOSED' 
                and i.crime_close_date is not null 
                and i.date_reported <= i.CRIME_CLOSE_DATE 
                AND i.date_reported < SYSDATE 
                AND i.CRIME_CLOSE_DATE < SYSDATE ) 
                THEN
                    insert into GOOD_STAGE_REPORTED_CRIME (crime_id,pk_report_id,date_reported,crime_category,crime_status,crime_close_date,fk1_officer_id,fk2_station_id,datasource)
                    values(i.crime_id,i.pk_report_id,i.date_reported,i.crime_category,i.crime_status,i.crime_close_date,i.fk1_officer_id,i.fk2_station_id,i.datasource);
    elsif(i.pk_report_id is not null 
                and i.date_reported is not null
                and i.crime_category is not null 
                and i.crime_status is not null 
                and i.fk1_officer_id is not null 
                and i.fk2_station_id is not null 
                AND UPPER(i.crime_status) ='ESCALATE'
                and i.crime_close_date is null  
                and i.date_reported < sysdate) 
                THEN
                    insert into GOOD_STAGE_REPORTED_CRIME (crime_id,pk_report_id,date_reported,crime_category,crime_status,crime_close_date,fk1_officer_id,fk2_station_id,datasource)
                    values(i.crime_id,i.pk_report_id,i.date_reported,i.crime_category,i.crime_status,i.crime_close_date,i.fk1_officer_id,i.fk2_station_id,i.datasource);
    ELSE
                    insert into BAD_STAGE_REPORTED_CRIME (crime_id,pk_report_id,date_reported,crime_category,crime_status,crime_close_date,fk1_officer_id,fk2_station_id,datasource)
                    values(i.crime_id,i.pk_report_id,i.date_reported,i.crime_category,i.crime_status,i.crime_close_date,i.fk1_officer_id,i.fk2_station_id,i.datasource);      
    END IF;    

    -----inserting into issue_log if any invalid data is found------  
        if (i.pk_report_id is null 
                OR i.date_reported is  null 
                OR i.crime_category is null 
                OR i.crime_status is  null 
                OR i.fk1_officer_id is  null 
                OR i.fk2_station_id is  null 
                OR (i.crime_status ='CLOSED' AND i.crime_close_date IS NULL)
               ) 
                THEN
                   INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
                   VALUES('NULL VALUE','NOT FIXED','stage_reported_crime',i.crime_id,CURRENT_TIMESTAMP,NULL);
         END IF;          
        if((i.crime_status IN('OPEN','ESCALATE') AND i.crime_close_date IS NOT NULL ) OR i.date_reported >i.crime_close_date OR i.date_reported > sysdate OR i.crime_close_date > sysdate) THEN
                   INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
                   VALUES('INVALID DATE','NOT FIXED','stage_reported_crime',i.crime_id,CURRENT_TIMESTAMP,NULL);
       END IF;
 END LOOP;
 Exception 
WHEN no_data_found THEN 
RAISE_APPLICATION_ERROR (-20001,'NO DATA FOUND');
END;
/ 

BEGIN
PR_SEPERATION_stage_reported_crime;
END;
/


--------- DATA SEPERATION FOR Police_employeee---------

CREATE OR REPLACE PROCEDURE PR_SEPERATION_Police_employee IS
CURSOR CURSOR_SEPERATION_Police_employee IS
SELECT * FROM Police_employee 
WHERE NOT EXISTS(
    SELECT GOOD_Police_employee.officer_id, BAD_Police_employee.officer_id FROM GOOD_Police_employee,BAD_Police_employee 
                 WHERE Police_employee.officer_id = GOOD_Police_employee.officer_id
                 OR Police_employee.officer_id = BAD_Police_employee.officer_id
               );

BEGIN
 FOR i IN CURSOR_SEPERATION_Police_employee LOOP
       IF(i.firstname is not null
           and i.lastname is not null
           and i.fk1_station_id is not null
           and not REGEXP_LIKE(i.firstname, '[#!$^&*%./\|]')
           and not REGEXP_LIKE(i.lastname, '[#!$^&*%./\|]')
           )THEN
                INSERT INTO GOOD_Police_employee(officer_id,pk_police_id,firstname,lastname,fk1_station_id,datasource)
                VALUES(i.officer_id,i.pk_police_id,i.firstname,i.lastname,i.fk1_station_id,i.datasource);
          ELSE 
                INSERT INTO BAD_Police_employee(officer_id,pk_police_id,firstname,lastname,fk1_station_id,datasource)
                VALUES(i.officer_id,i.pk_police_id,i.firstname,i.lastname,i.fk1_station_id,i.datasource);
       END IF;

        -----inserting into issue_log if any invalid data is found------  
    
        IF(i.firstname is null
           OR i.lastname is null
           OR i.fk1_station_id is null
               ) 
                THEN
                   INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
                   VALUES('NULL VALUE','NOT FIXED','Police_employee',i.OFFICER_ID,CURRENT_TIMESTAMP,NULL);
       END IF;      
        if(REGEXP_LIKE(i.firstname, '[#!$^&@*%./\|]')
           OR REGEXP_LIKE(i.lastname, '[#!$^&@*%./\|]')
           ) 
           THEN
                   INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
                   VALUES('SPECIAL CHARACTER FOUND','NOT FIXED','Police_employee',i.OFFICER_ID,CURRENT_TIMESTAMP,NULL);
       END IF;
 END LOOP;
END;
/

BEGIN
PR_SEPERATION_Police_employee;
END;
/


--------- DATA SEPERATION FOR Police_employeee---------

CREATE OR REPLACE PROCEDURE PR_SEPERATION_Station IS
CURSOR CURSOR_SEPERATION_Station IS
SELECT * FROM Station 
WHERE NOT EXISTS(
    SELECT GOOD_Station.station_no, BAD_Station.station_no
    FROM GOOD_Station ,BAD_Station
    WHERE Station.station_no = GOOD_Station.station_no
    OR Station.station_no = BAD_Station.station_no
);
BEGIN 
FOR i IN CURSOR_SEPERATION_Station LOOP
  IF(i.pk_station_id is not null
      AND i.station_name is not null
      AND i.region is not null
      and not REGEXP_LIKE(i.station_name, '[#!$^&@*%./\|]')
      and not REGEXP_LIKE(i.region, '[#!$^&@*%./\|]'))
      THEN
          INSERT INTO GOOD_Station(station_no,pk_station_id,station_name,region,datasource)
          VALUES (i.station_no,i.pk_station_id,i.station_name,i.region,i.datasource);
    ELSE
       INSERT INTO BAD_Station(station_no,pk_station_id,station_name,region,datasource)
          VALUES (i.station_no,i.pk_station_id,i.station_name,i.region,i.datasource);
  END IF;
  
  -----inserting into issue_log if any invalid data is found------  
      IF(i.pk_station_id is null
         OR i.station_name is null
         OR i.region is null)
      THEN 
         INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
               VALUES('NULL VALUE','NOT FIXED','Station',i.STATION_NO,CURRENT_TIMESTAMP,NULL);
      END IF;

      IF(REGEXP_LIKE(i.station_name, '[#!$^&@*%./\|]')
         OR REGEXP_LIKE(i.region, '[#!$^&@*%./\|]'))  
         THEN 
             INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
               VALUES('SPECIAL CHARACTER FOUND','NOT FIXED','Station',i.STATION_NO,CURRENT_TIMESTAMP,NULL);
      END IF;
END LOOP;
END;
/

BEGIN
PR_SEPERATION_Station;
END;
/


--------- DATA SEPERATION FOR Crime_details---------

CREATE OR REPLACE PROCEDURE PR_SEPERATION_Crime_details IS
CURSOR CURSOR_SEPERATION_Crime_details IS
SELECT * FROM Crime_details;

BEGIN
FOR i IN CURSOR_SEPERATION_Crime_details LOOP
    IF(i.crime_id is not null
    AND i.officer_id is not null
    AND i.work_start_date is not null
    AND i.work_end_date is not null
    AND i.work_start_date <= i.work_end_date
    AND i.work_start_date < sysdate
    AND i.work_end_date <sysdate)
    THEN
        INSERT INTO GOOD_Crime_details(c_detail_id,crime_id,officer_id,work_start_date,work_end_date,datasource)
        VALUES(i.c_detail_id,i.crime_id,i.officer_id,i.work_start_date,i.work_end_date,i.datasource);

        ELSE
           INSERT INTO BAD_Crime_details(c_detail_id,crime_id,officer_id,work_start_date,work_end_date,datasource)
        VALUES(i.c_detail_id,i.crime_id,i.officer_id,i.work_start_date,i.work_end_date,i.datasource);
    END IF;

    IF(i.crime_id is null
    OR i.officer_id is null
    OR i.work_start_date is null
    OR i.work_end_date is null) 
    THEN
       INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
               VALUES('NULL VALUE','NOT FIXED','Crime_details',i.c_detail_id,CURRENT_TIMESTAMP,NULL);
    END IF;

    IF(i.work_start_date > i.work_end_date
    OR i.work_start_date > sysdate
    OR i.work_end_date > sysdate)
    THEN
       INSERT INTO issue_log(issue_desc,issue_status,issue_table,table_key,time,date_fixed)
                   VALUES('INVALID DATE','NOT FIXED','Crime_details',i.c_detail_id,CURRENT_TIMESTAMP,NULL);
    END IF;
END LOOP;
END;
/

BEGIN
PR_SEPERATION_Crime_details;
END;
/


