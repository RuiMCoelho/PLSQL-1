create table item(
    item varchar2(25) not null,
    dept number(4) not null,
    item_desc varchar2(25) not null
);

create table loc(
    loc number(10) not null,
    loc_desc varchar2(25) not null
);

CREATE TABLE item_loc_soh (
    item VARCHAR2(25) NOT NULL,
    loc NUMBER(10) NOT NULL,
    dept NUMBER(4) NOT NULL,
    unit_cost NUMBER(20,4) NOT NULL,
    stock_on_hand NUMBER(12,4) NOT NULL
);

--- in average this will take 1s to be executed
insert into item(item,dept,item_desc)
select level, round(DBMS_RANDOM.value(1,100)), translate(dbms_random.string('a', 20), 'abcXYZ', level) from dual connect by level <= 10000;

--- in average this will take 1s to be executed
insert into loc(loc,loc_desc)
select level+100, translate(dbms_random.string('a', 20), 'abcXYZ', level) from dual connect by level <= 1000;

-- in average this will take less than 120s to be executed
insert into item_loc_soh (item, loc, dept, unit_cost, stock_on_hand)
select item, loc, dept, (DBMS_RANDOM.value(5000,50000)), round(DBMS_RANDOM.value(1000,100000))
from item, loc; --ORA-01536: space quota exceeded for tablespace 'APEX_BIGFILE_INSTANCE_TBS4'

select count(1) from item, loc; --10.000.000

insert into item_loc_soh (item, loc, dept, unit_cost, stock_on_hand)
select item, loc, dept, (DBMS_RANDOM.value(5000,50000)), round(DBMS_RANDOM.value(1000,100000))
from item, loc
where rownum <= 100000;

commit;

-------------------------------------------------------------------------------------------------------------
--1 - Primary key definition and any other constraint or index suggestion
--
--table item should have a primary key in the 'item' column
	ALTER TABLE item ADD CONSTRAINT ITEM_PK PRIMARY KEY (item);
--table loc should have a primary key in the 'loc' column
	ALTER TABLE loc ADD CONSTRAINT LOC_PK PRIMARY KEY (loc);
--table item_loc_soh should have a primary key in the 'loc','dept' and 'item' columns. As there are frequent queries by department a global index should be added as well
	ALTER TABLE item_loc_soh ADD CONSTRAINT ITEM_LOC_SOH_PK PRIMARY KEY (loc,dept,item);
	CREATE INDEX item_loc_soh_idx_1 ON item_loc_soh (dept);
-------------------------------------------------------------------------------------------------------------	
--2 - Your suggestion for table data management and data access considering the application usage, for example, partition...
--
--table item_loc_soh should be partitioned to increase query performance. My suggestion would be to add to the create table statemen the following:
	CREATE TABLE item_loc_soh (
		item VARCHAR2(25) NOT NULL,
		loc NUMBER(10) NOT NULL,
		dept NUMBER(4) NOT NULL,
		unit_cost NUMBER(20,4) NOT NULL,
		stock_on_hand NUMBER(12,4) NOT NULL
	)
	PARTITION BY RANGE (loc) INTERVAL (1)
	SUBPARTITION BY HASH (dept) SUBPARTITIONS 5
	(
		PARTITION p0 VALUES LESS THAN (20)
	);
--this allows for data pruning on queries accessing each store, and the hash subpartition distributes the data more evenly across partitions to help on queries to specific departments.
--It also makes it easier to add new stores to the company, or remove old ones, as partitions are automatically created or can be easily archived, respectively.
--Would also add a local index to item, for direct accessing
	CREATE INDEX item_loc_soh_idx_local_1 ON item_loc_soh (item) LOCAL;
-------------------------------------------------------------------------------------------------------------
--3 - Your suggestion to avoid row contention at table level parameter because of high level of concurrency
--
--The application lack of write statements and the previous partition data pruning on queries takes care of most of the problems, 
--assuming queries avoid full table scans and updates lock only required rows. If a problem we may look into tuning the INITRANS value for the item_loc_soh table
-------------------------------------------------------------------------------------------------------------
--4 - Create a view that can be used at screen level to show only the required fields
--
--Assuming the required fields item,loc,dept,cost and stock, and that a loc must be provided at screen level, we can create the following view 
	CREATE OR REPLACE VIEW item_loc_soh_view AS
	SELECT 
		item, 
		loc, 
		dept, 
		unit_cost AS cost, 
		stock_on_hand AS stock
	FROM 
		item_loc_soh;
-------------------------------------------------------------------------------------------------------------
--5 - Create a new table that associates user to existing dept(s)
--
--assuming the existence of another dimension table that stores the users information, we can create a mapping table between user identifier and department identifier as such:
	create table user_depts(
    user_id number(5) not null,
    dept number(4) not null,
	CONSTRAINT user_depts_pk PRIMARY KEY (user_id, dept)
	);
-------------------------------------------------------------------------------------------------------------
--6 - Create a package with procedure or function that can be invoked by store or all stores to save the item_loc_soh to a 
--new table that will contain the same information plus the stock value per item/loc (unit_cost*stock_on_hand)
--
--First we create the new table item_loc_soh_val. Assuming we will use this table now, the same considerations of indexing and partitioning apply..
	CREATE TABLE item_loc_soh_val (
    item VARCHAR2(25) NOT NULL,
    loc NUMBER(10) NOT NULL,
    dept NUMBER(4) NOT NULL,
    unit_cost NUMBER(20,4) NOT NULL,
    stock_on_hand NUMBER(12,4) NOT NULL,
    stock_value NUMBER(32,8) NOT NULL,
	CONSTRAINT ITEM_LOC_SOH_VAL_PK PRIMARY KEY (loc,dept,item)
	)
	PARTITION BY RANGE (loc) INTERVAL (1)
	SUBPARTITION BY HASH (dept) SUBPARTITIONS 5
	(
		PARTITION p0 VALUES LESS THAN (20)
	);
	
	CREATE INDEX item_loc_soh_val_idx_1 ON item_loc_soh_val (dept);	
    CREATE INDEX item_loc_soh_val_idx_local_1 ON item_loc_soh (item) LOCAL;
	
--creating now the package...
	CREATE OR REPLACE PACKAGE item_loc_pkg AS
	  
	  PROCEDURE save_item_loc_data(p_loc IN NUMBER default NULL);
	  
	END item_loc_pkg;

	CREATE OR REPLACE PACKAGE BODY item_loc_pkg AS
	  --
	  PROCEDURE save_item_loc_data(p_loc IN NUMBER default NULL) IS
	  BEGIN
	    --	
		INSERT /*+ APPEND */ INTO item_loc_soh_val (item, loc, dept, unit_cost, stock_on_hand, stock_value)
		SELECT 
		  i.item,
		  i.loc,
		  i.dept,
		  i.unit_cost,
		  i.stock_on_hand,
		  (i.unit_cost * i.stock_on_hand) AS stock_value
		FROM 
		  item_loc_soh i
		WHERE 
		  i.loc = nvl(p_loc,i.loc);
		--
		COMMIT;
		--
	  EXCEPTION
		WHEN OTHERS THEN
		  --
		  ROLLBACK;
		  --
		  RAISE;
		  --
	  END save_item_loc_data;
      --
	END item_loc_pkg;	
-------------------------------------------------------------------------------------------------------------
--7 - Create a data filter mechanism that can be used at screen level to filter out the data that user can see accordingly to dept association (created previously)
--
--We can simply alter the previously created view (that is used to send data to the application) to filter according to logged user as such:
	CREATE OR REPLACE VIEW item_loc_soh_view AS
	SELECT 
		s.item, 
		s.loc, 
		s.dept, 
		s.unit_cost AS cost, 
		s.stock_on_hand AS stock
	FROM 
		item_loc_soh s,
		user_depts d 
	WHERE s.dept = d.dept
	  AND d.user_id = SYS_CONTEXT('USERENV', 'SESSION_USER');
--if the application grows more complex, we can use the DBMS_RLS package to institute a security policy with a reusable function for several tables. this policy function would only need 
--to return a varchar with 'dept in (' || list_of_user_deps || ')' to be used in DBMS_RLS.ADD_POLICY (assuming column naming convention is enforced in the datamodel for the column dept)
-------------------------------------------------------------------------------------------------------------
--8 - Create a pipeline function to be used in the location list of values (drop down)
--
--First we must create the types to represent the location data as such:
	CREATE OR REPLACE TYPE location_obj AS OBJECT (loc NUMBER(10),loc_desc VARCHAR2(25));
	CREATE OR REPLACE TYPE location_table AS TABLE OF location_obj;
	
--we can then create the required function:
	CREATE OR REPLACE FUNCTION get_locations RETURN location_table PIPELINED AS
	--
	BEGIN
		--
		FOR loc_row IN (SELECT loc, loc_desc FROM loc ORDER BY loc_desc) LOOP
			--
			PIPE ROW (location_obj(loc_row.loc, loc_row.loc_desc));
			--
		END LOOP;
		--
		RETURN;
		--
	END get_location_lov;

--the data can then be retrieved with a query similar to:
	SELECT * FROM TABLE(get_locations);
-------------------------------------------------------------------------------------------------------------
--9 - Looking into the following explain plan what should be your recommendation and implementation to improve the existing data model. Please share your solution in sql and the 
--corresponding explain plan of that solution. Please take in consideration the way that user will use the app.
--
--Unfortunately Apex does not allow me to fill in the tables in accordance to the provided script, throwing a ORA-01536: space quota exceeded for tablespace 'APEX_BIGFILE_INSTANCE_TBS4'.
--However, the table access full in the select of the explain plan provided can be avoided implementing the indexes e and partitioning indicated in previous questions. The new explain plan would then
-- be a TABLE ACCESS BY GLOBAL INDEX ROWID BATCHED using a INDEX RANGE SCAN of the primary key
-------------------------------------------------------------------------------------------------------------
--10 - Run the previous method that was created on 6. for all the stores from item_loc_soh to the history table. The entire migration should not take more than 10s to run 
--(don't use parallel hint to solve it :))
--
--To run it one must simply execute item_loc_pkg.save_item_loc_data(). I cannot test its performance due to the ORA-01536: space quota exceeded for tablespace previously mentioned.
--Trying to work around the limitation, by removing partitions from tables and limiting the records on item_loc_soh to 100000 (from the initial 10 milions) the execution takes less than 1 second
-------------------------------------------------------------------------------------------------------------
--11 - Please have a look into the AWR report (AWR.html) in attachment and let us know what is the problem that the AWR is highlighting and potential solution.
--
--The main issue is the contention on cpu (wait event resmgr:cpu quantum). CPU intensive queries should be tunned and, ultimately actual CPU resources adjusted. We can also see that sql execute elapsed time is
-- 99.55% of DB time, further indicating the need for SQL tunning. Briefly looking into some queries, we see table full accesses that confirm the need for a full review of the data model in terms of 
--usage of indexes, partitioning besides the previously mentioned tunning of the queries.
-------------------------------------------------------------------------------------------------------------
--12 - Create a program (plsql and/or java, or any other language) that can extract to a flat file (csv), 1 file per location: the item, department unit cost, stock on hand quantity and stock value. 
--Creating the 1000 files should take less than 30s.
--
--The functionality can be accomplished in pl/sql, as it typically was done in legacy systems for external integration. We only require the existence of a directory in the database where we can write 
--files to. lets assume its name of csv_dir...
	DECLARE
		--
		file_handle UTL_FILE.FILE_TYPE;
		v_file_name VARCHAR2(100);    
		--
		CURSOR loc_cur IS
			SELECT DISTINCT loc FROM item_loc_soh;    
		--
		TYPE item_loc_type IS RECORD (
			item VARCHAR2(25),
			dept NUMBER(4),
			unit_cost NUMBER(20, 4),
			stock_on_hand NUMBER(12, 4)
		);    
		--
		TYPE item_loc_table_type IS TABLE OF item_loc_type;
		v_item_loc_data item_loc_table_type;
		--
		v_loc NUMBER(10);
		--
		c_directory CONSTANT VARCHAR2(30) := 'CSV_DIR';
		-- Timer
		v_start_time NUMBER;
		v_end_time NUMBER;
		--
	BEGIN
		--
		v_start_time := DBMS_UTILITY.GET_TIME;    
		-- loop on each location
		FOR loc_rec IN loc_cur LOOP
			--
			v_loc := loc_rec.loc;
			--
			v_file_name := 'location_' || v_loc || '.csv';
			file_handle := UTL_FILE.FOPEN(c_directory, v_file_name, 'W');
			--
			UTL_FILE.PUT_LINE(file_handle, 'ITEM,DEPT,UNIT_COST,STOCK_ON_HAND');
			--
			SELECT item, dept, unit_cost, stock_on_hand
			BULK COLLECT INTO v_item_loc_data
			FROM item_loc_soh
			WHERE loc = v_loc;
			-- write each row to the file
			FOR i IN 1..v_item_loc_data.COUNT LOOP
				--
				UTL_FILE.PUT_LINE(file_handle, 
								  v_item_loc_data(i).item || ',' || 
								  v_item_loc_data(i).dept || ',' || 
								  v_item_loc_data(i).unit_cost || ',' || 
								  v_item_loc_data(i).stock_on_hand);
				--
			END LOOP;
			--
			UTL_FILE.FCLOSE(file_handle);
			--
		END LOOP;
		--
		v_end_time := DBMS_UTILITY.GET_TIME;
		--
		DBMS_OUTPUT.PUT_LINE('Time taken: ' || ((v_end_time - v_start_time) / 100) || ' seconds');
		--
	EXCEPTION
		--
		WHEN OTHERS THEN
			--
			IF UTL_FILE.IS_OPEN(file_handle) THEN
				--
				UTL_FILE.FCLOSE(file_handle);
				--
			END IF;
			--
			RAISE;
		--
	END;
	/
-------------------------------------------------------------------------------------------------------------




