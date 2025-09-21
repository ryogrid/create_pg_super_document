DESCRIBE  
---  
[Prev](ecpg-sql-declare-statement.md "DECLARE STATEMENT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-disconnect.md "DISCONNECT")  
  
* * *

## DESCRIBE

DESCRIBE — obtain information about a prepared statement or result set

## Synopsis
    
    
    DESCRIBE [ OUTPUT ] _prepared_name_ USING [ SQL ] DESCRIPTOR _descriptor_name_
    DESCRIBE [ OUTPUT ] _prepared_name_ INTO [ SQL ] DESCRIPTOR _descriptor_name_
    DESCRIBE [ OUTPUT ] _prepared_name_ INTO _sqlda_name_
    

## Description

`DESCRIBE` retrieves metadata information about the result columns contained in a prepared statement, without actually fetching a row. 

## Parameters

 _`prepared_name`_ #
    

The name of a prepared statement. This can be an SQL identifier or a host variable. 

_`descriptor_name`_ #
    

A descriptor name. It is case sensitive. It can be an SQL identifier or a host variable. 

_`sqlda_name`_ #
    

The name of an SQLDA variable. 

## Examples
    
    
    EXEC SQL ALLOCATE DESCRIPTOR mydesc;
    EXEC SQL PREPARE stmt1 FROM :sql_stmt;
    EXEC SQL DESCRIBE stmt1 INTO SQL DESCRIPTOR mydesc;
    EXEC SQL GET DESCRIPTOR mydesc VALUE 1 :charvar = NAME;
    EXEC SQL DEALLOCATE DESCRIPTOR mydesc;
    

## Compatibility

`DESCRIBE` is specified in the SQL standard. 

## See Also

[ALLOCATE DESCRIPTOR](ecpg-sql-allocate-descriptor.md "ALLOCATE DESCRIPTOR"), [GET DESCRIPTOR](ecpg-sql-get-descriptor.md "GET DESCRIPTOR")

* * *

[Prev](ecpg-sql-declare-statement.md "DECLARE STATEMENT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-disconnect.md "DISCONNECT")  
---|---|---  
DECLARE STATEMENT | [Home](index.md "PostgreSQL 17.5 Documentation")|  DISCONNECT
