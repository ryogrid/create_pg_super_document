DECLARE  
---  
[Prev](ecpg-sql-deallocate-descriptor.md "DEALLOCATE DESCRIPTOR") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-declare-statement.md "DECLARE STATEMENT")  
  
* * *

## DECLARE

DECLARE — define a cursor

## Synopsis
    
    
    DECLARE _cursor_name_ [ BINARY ] [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ] CURSOR [ { WITH | WITHOUT } HOLD ] FOR _prepared_name_
    DECLARE _cursor_name_ [ BINARY ] [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ] CURSOR [ { WITH | WITHOUT } HOLD ] FOR _query_
    

## Description

`DECLARE` declares a cursor for iterating over the result set of a prepared statement. This command has slightly different semantics from the direct SQL command `DECLARE`: Whereas the latter executes a query and prepares the result set for retrieval, this embedded SQL command merely declares a name as a “loop variable” for iterating over the result set of a query; the actual execution happens when the cursor is opened with the `OPEN` command. 

## Parameters

 _`cursor_name`_ #
    

A cursor name, case sensitive. This can be an SQL identifier or a host variable. 

_`prepared_name`_ #
    

The name of a prepared query, either as an SQL identifier or a host variable. 

_`query`_ #
    

A [SELECT](sql-select.md "SELECT") or [VALUES](sql-values.md "VALUES") command which will provide the rows to be returned by the cursor. 

For the meaning of the cursor options, see [DECLARE](sql-declare.md "DECLARE"). 

## Examples

Examples declaring a cursor for a query: 
    
    
    EXEC SQL DECLARE C CURSOR FOR SELECT * FROM My_Table;
    EXEC SQL DECLARE C CURSOR FOR SELECT Item1 FROM T;
    EXEC SQL DECLARE cur1 CURSOR FOR SELECT version();
    

An example declaring a cursor for a prepared statement: 
    
    
    EXEC SQL PREPARE stmt1 AS SELECT version();
    EXEC SQL DECLARE cur1 CURSOR FOR stmt1;
    

## Compatibility

`DECLARE` is specified in the SQL standard. 

## See Also

[OPEN](ecpg-sql-open.md "OPEN"), [CLOSE](sql-close.md "CLOSE"), [DECLARE](sql-declare.md "DECLARE")

* * *

[Prev](ecpg-sql-deallocate-descriptor.md "DEALLOCATE DESCRIPTOR") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-declare-statement.md "DECLARE STATEMENT")  
---|---|---  
DEALLOCATE DESCRIPTOR | [Home](index.md "PostgreSQL 17.5 Documentation")|  DECLARE STATEMENT
