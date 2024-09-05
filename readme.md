# Instructions
- Create account in https://apex.oracle.com/en/
- Request Workspace
- Run the following to create the data model

```sql
create table item(
    item varchar2(25) not null,
    dept number(4) not null,
    item_desc varchar2(25) not null
);

create table loc(
    loc number(10) not null,
    loc_desc varchar2(25) not null
);

create table item_loc_soh(
item varchar2(25) not null,
loc number(10) not null,
dept number(4) not null,
unit_cost number(20,4) not null,
stock_on_hand number(12,4) not null
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
from item, loc;

commit;
```
- in the Apex App Builder import the application StockApplication.sql. When opening the application for login will be the same email and password as registered at apex.oracle.com.


**NOTE: If you fail to complete any challenge please still reply with what was the thinking and why you were not able to complete.**


[INFO]
> After finishing the problems, create a public repository in Github, push your code and send us the Github Public URL.
> If for some reason this is not possible, send us a zip folder containing a install script with all the required solution identified.
> The source contains a StockApplication.zip Apex application that can be deployed to better understand the challenge from user perspective.

# Context
Item loc stock an hand represents a snapshot table of stock in a specific moment for all items in all stores/warehouses for a retailer. In scenario where you have an Apex application that enables a view of stock per store/warehouse please consider the following:
 - this application has an very high user concurrency access during the entire day
 - the access to the application data is per store/warehouse
 - one of the attributes that most store/warehouse users search is by dept
 
# Challenge
## Must Have
### Data Model
Please consider that your reply for each point should include an explanation and corresponding sql code 
1. Primary key definition and any other constraint or index suggestion
2. Your suggestion for table data management and data access considering the application usage, for example, partition...
3. Your suggestion to avoid row contention at table level parameter because of high level of concurrency
4. Create a view that can be used at screen level to show only the required fields
5. Create a new table that associates user to existing dept(s)

### PLSQL Development
6. Create a package with procedure or function that can be invoked by store or all stores to save the item_loc_soh to a new table that will contain the same information plus the stock value per item/loc (unit_cost*stock_on_hand)
7. Create a data filter mechanism that can be used at screen level to filter out the data that user can see accordingly to dept association (created previously)
8. Create a pipeline function to be used in the location list of values (drop down)

## Should Have
### Performance
9. Looking into the following explain plan what should be your recommendation and implementation to improve the existing data model. Please share your solution in sql and the corresponding explain plan of that solution. Please take in consideration the way that user will use the app.
```sql

 Plan Hash Value  : 1697218418 

------------------------------------------------------------------------------
| Id  | Operation           | Name         | Rows | Bytes | Cost  | Time       |
------------------------------------------------------------------------------
|   0 | SELECT STATEMENT    |              | 100019 | 40760 | 10840 | 00:00:03 |
| * 1 |   TABLE ACCESS FULL | ITEM_LOC_SOH | 100019 | 40760 | 10840 | 00:00:03 |
------------------------------------------------------------------------------

Predicate Information (identified by operation id):
------------------------------------------
* 1 - filter("LOC"=652 AND "DEPT"=68)


Notes
-----
- Dynamic sampling used for this statement ( level = 2 )

```


 10. Run the previous method that was created on 6. for all the stores from item_loc_soh to the history table. The entire migration should not take more than 10s to run (don't use parallel hint to solve it :)) 
 11. Please have a look into the AWR report (AWR.html) in attachment and let us know what is the problem that the AWR is highlighting and potential solution.

## Nice to have
### Performance
11. Create a program (plsql and/or java, or any other language) that can extract to a flat file (csv), 1 file per location: the item, department unit cost, stock on hand quantity and stock value.
Creating the 1000 files should take less than 30s.
 
 

