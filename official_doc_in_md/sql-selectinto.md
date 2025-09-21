SELECT INTO  
---  
[Prev](sql-select.md "SELECT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-set.md "SET")  
  
* * *

## SELECT INTO

SELECT INTO — define a new table from the results of a query

## Synopsis
    
    
    [ WITH [ RECURSIVE ] _with_query_ [, ...] ]
    SELECT [ ALL | DISTINCT [ ON ( _expression_ [, ...] ) ] ]
        [ { * | _expression_ [ [ AS ] _output_name_ ] } [, ...] ]
        INTO [ TEMPORARY | TEMP | UNLOGGED ] [ TABLE ] _new_table_
        [ FROM _from_item_ [, ...] ]
        [ WHERE _condition_ ]
        [ GROUP BY _expression_ [, ...] ]
        [ HAVING _condition_ ]
        [ WINDOW _window_name_ AS ( _window_definition_ ) [, ...] ]
        [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] _select_ ]
        [ ORDER BY _expression_ [ ASC | DESC | USING _operator_ ] [ NULLS { FIRST | LAST } ] [, ...] ]
        [ LIMIT { _count_ | ALL } ]
        [ OFFSET _start_ [ ROW | ROWS ] ]
        [ FETCH { FIRST | NEXT } [ _count_ ] { ROW | ROWS } ONLY ]
        [ FOR { UPDATE | SHARE } [ OF _table_name_ [, ...] ] [ NOWAIT ] [...] ]
    

## Description

`SELECT INTO` creates a new table and fills it with data computed by a query. The data is not returned to the client, as it is with a normal `SELECT`. The new table's columns have the names and data types associated with the output columns of the `SELECT`. 

## Parameters

`TEMPORARY` or `TEMP`
    

If specified, the table is created as a temporary table. Refer to [CREATE TABLE](sql-createtable.md "CREATE TABLE") for details. 

`UNLOGGED`
    

If specified, the table is created as an unlogged table. Refer to [CREATE TABLE](sql-createtable.md "CREATE TABLE") for details. 

_`new_table`_
    

The name (optionally schema-qualified) of the table to be created. 

All other parameters are described in detail under [SELECT](sql-select.md "SELECT"). 

## Notes

[`CREATE TABLE AS`](sql-createtableas.md "CREATE TABLE AS") is functionally similar to `SELECT INTO`. `CREATE TABLE AS` is the recommended syntax, since this form of `SELECT INTO` is not available in ECPG or PL/pgSQL, because they interpret the `INTO` clause differently. Furthermore, `CREATE TABLE AS` offers a superset of the functionality provided by `SELECT INTO`. 

In contrast to `CREATE TABLE AS`, `SELECT INTO` does not allow specifying properties like a table's access method with [`USING _`method`_`](sql-createtable.md#SQL-CREATETABLE-METHOD) or the table's tablespace with [`TABLESPACE _`tablespace_name`_`](sql-createtable.md#SQL-CREATETABLE-TABLESPACE). Use `CREATE TABLE AS` if necessary. Therefore, the default table access method is chosen for the new table. See [default_table_access_method](runtime-config-client.md#GUC-DEFAULT-TABLE-ACCESS-METHOD) for more information. 

## Examples

Create a new table `films_recent` consisting of only recent entries from the table `films`: 
    
    
    SELECT * INTO films_recent FROM films WHERE date_prod >= '2002-01-01';
    

## Compatibility

The SQL standard uses `SELECT INTO` to represent selecting values into scalar variables of a host program, rather than creating a new table. This indeed is the usage found in ECPG (see [Chapter 34](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")) and PL/pgSQL (see [Chapter 41](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language")). The PostgreSQL usage of `SELECT INTO` to represent table creation is historical. Some other SQL implementations also use `SELECT INTO` in this way (but most SQL implementations support `CREATE TABLE AS` instead). Apart from such compatibility considerations, it is best to use `CREATE TABLE AS` for this purpose in new code. 

## See Also

[CREATE TABLE AS](sql-createtableas.md "CREATE TABLE AS")

* * *

[Prev](sql-select.md "SELECT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-set.md "SET")  
---|---|---  
SELECT | [Home](index.md "PostgreSQL 17.5 Documentation")|  SET
