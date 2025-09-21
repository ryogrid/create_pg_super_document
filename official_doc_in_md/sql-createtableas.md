CREATE TABLE AS  
---  
[Prev](sql-createtable.md "CREATE TABLE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-createtablespace.md "CREATE TABLESPACE")  
  
* * *

## CREATE TABLE AS

CREATE TABLE AS — define a new table from the results of a query

## Synopsis
    
    
    CREATE [ [ GLOBAL | LOCAL ] { TEMPORARY | TEMP } | UNLOGGED ] TABLE [ IF NOT EXISTS ] _table_name_
        [ (_column_name_ [, ...] ) ]
        [ USING _method_ ]
        [ WITH ( _storage_parameter_ [= _value_] [, ... ] ) | WITHOUT OIDS ]
        [ ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP } ]
        [ TABLESPACE _tablespace_name_ ]
        AS _query_
        [ WITH [ NO ] DATA ]
    

## Description

`CREATE TABLE AS` creates a table and fills it with data computed by a `SELECT` command. The table columns have the names and data types associated with the output columns of the `SELECT` (except that you can override the column names by giving an explicit list of new column names). 

`CREATE TABLE AS` bears some resemblance to creating a view, but it is really quite different: it creates a new table and evaluates the query just once to fill the new table initially. The new table will not track subsequent changes to the source tables of the query. In contrast, a view re-evaluates its defining `SELECT` statement whenever it is queried. 

`CREATE TABLE AS` requires `CREATE` privilege on the schema used for the table. 

## Parameters

`GLOBAL` or `LOCAL`
    

Ignored for compatibility. Use of these keywords is deprecated; refer to [CREATE TABLE](sql-createtable.md "CREATE TABLE") for details. 

`TEMPORARY` or `TEMP`
    

If specified, the table is created as a temporary table. Refer to [CREATE TABLE](sql-createtable.md "CREATE TABLE") for details. 

`UNLOGGED`
    

If specified, the table is created as an unlogged table. Refer to [CREATE TABLE](sql-createtable.md "CREATE TABLE") for details. 

`IF NOT EXISTS`
    

Do not throw an error if a relation with the same name already exists; simply issue a notice and leave the table unmodified. 

_`table_name`_
    

The name (optionally schema-qualified) of the table to be created. 

_`column_name`_
    

The name of a column in the new table. If column names are not provided, they are taken from the output column names of the query. 

`USING _`method`_`
    

This optional clause specifies the table access method to use to store the contents for the new table; the method needs be an access method of type `TABLE`. See [Chapter 61](tableam.md "Chapter 61. Table Access Method Interface Definition") for more information. If this option is not specified, the default table access method is chosen for the new table. See [default_table_access_method](runtime-config-client.md#GUC-DEFAULT-TABLE-ACCESS-METHOD) for more information. 

`WITH ( _`storage_parameter`_ [= _`value`_] [, ... ] )`
    

This clause specifies optional storage parameters for the new table; see [Storage Parameters](sql-createtable.md#SQL-CREATETABLE-STORAGE-PARAMETERS "Storage Parameters") in the [CREATE TABLE](sql-createtable.md "CREATE TABLE") documentation for more information. For backward-compatibility the `WITH` clause for a table can also include `OIDS=FALSE` to specify that rows of the new table should contain no OIDs (object identifiers), `OIDS=TRUE` is not supported anymore. 

`WITHOUT OIDS`
    

This is backward-compatible syntax for declaring a table `WITHOUT OIDS`, creating a table `WITH OIDS` is not supported anymore. 

`ON COMMIT`
    

The behavior of temporary tables at the end of a transaction block can be controlled using `ON COMMIT`. The three options are: 

`PRESERVE ROWS`
    

No special action is taken at the ends of transactions. This is the default behavior. 

`DELETE ROWS`
    

All rows in the temporary table will be deleted at the end of each transaction block. Essentially, an automatic [`TRUNCATE`](sql-truncate.md "TRUNCATE") is done at each commit. 

`DROP`
    

The temporary table will be dropped at the end of the current transaction block. 

`TABLESPACE _`tablespace_name`_`
    

The _`tablespace_name`_ is the name of the tablespace in which the new table is to be created. If not specified, [default_tablespace](runtime-config-client.md#GUC-DEFAULT-TABLESPACE) is consulted, or [temp_tablespaces](runtime-config-client.md#GUC-TEMP-TABLESPACES) if the table is temporary. 

_`query`_
    

A [`SELECT`](sql-select.md "SELECT"), [`TABLE`](sql-select.md#SQL-TABLE "TABLE Command"), or [`VALUES`](sql-values.md "VALUES") command, or an [`EXECUTE`](sql-execute.md "EXECUTE") command that runs a prepared `SELECT`, `TABLE`, or `VALUES` query. 

`WITH [ NO ] DATA`
    

This clause specifies whether or not the data produced by the query should be copied into the new table. If not, only the table structure is copied. The default is to copy the data. 

## Notes

This command is functionally similar to [SELECT INTO](sql-selectinto.md "SELECT INTO"), but it is preferred since it is less likely to be confused with other uses of the `SELECT INTO` syntax. Furthermore, `CREATE TABLE AS` offers a superset of the functionality offered by `SELECT INTO`. 

## Examples

Create a new table `films_recent` consisting of only recent entries from the table `films`: 
    
    
    CREATE TABLE films_recent AS
      SELECT * FROM films WHERE date_prod >= '2002-01-01';
    

To copy a table completely, the short form using the `TABLE` command can also be used: 
    
    
    CREATE TABLE films2 AS
      TABLE films;
    

Create a new temporary table `films_recent`, consisting of only recent entries from the table `films`, using a prepared statement. The new table will be dropped at commit: 
    
    
    PREPARE recentfilms(date) AS
      SELECT * FROM films WHERE date_prod > $1;
    CREATE TEMP TABLE films_recent ON COMMIT DROP AS
      EXECUTE recentfilms('2002-01-01');
    

## Compatibility

`CREATE TABLE AS` conforms to the SQL standard. The following are nonstandard extensions: 

  * The standard requires parentheses around the subquery clause; in PostgreSQL, these parentheses are optional. 

  * In the standard, the `WITH [ NO ] DATA` clause is required; in PostgreSQL it is optional. 

  * PostgreSQL handles temporary tables in a way rather different from the standard; see [CREATE TABLE](sql-createtable.md "CREATE TABLE") for details. 

  * The `WITH` clause is a PostgreSQL extension; storage parameters are not in the standard. 

  * The PostgreSQL concept of tablespaces is not part of the standard. Hence, the clause `TABLESPACE` is an extension. 




## See Also

[CREATE MATERIALIZED VIEW](sql-creatematerializedview.md "CREATE MATERIALIZED VIEW"), [CREATE TABLE](sql-createtable.md "CREATE TABLE"), [EXECUTE](sql-execute.md "EXECUTE"), [SELECT](sql-select.md "SELECT"), [SELECT INTO](sql-selectinto.md "SELECT INTO"), [VALUES](sql-values.md "VALUES")

* * *

[Prev](sql-createtable.md "CREATE TABLE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-createtablespace.md "CREATE TABLESPACE")  
---|---|---  
CREATE TABLE | [Home](index.md "PostgreSQL 17.5 Documentation")|  CREATE TABLESPACE
