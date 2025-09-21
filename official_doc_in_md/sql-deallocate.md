DEALLOCATE  
---  
[Prev](sql-createview.md "CREATE VIEW") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-declare.md "DECLARE")  
  
* * *

## DEALLOCATE

DEALLOCATE — deallocate a prepared statement

## Synopsis
    
    
    DEALLOCATE [ PREPARE ] { _name_ | ALL }
    

## Description

`DEALLOCATE` is used to deallocate a previously prepared SQL statement. If you do not explicitly deallocate a prepared statement, it is deallocated when the session ends. 

For more information on prepared statements, see [PREPARE](sql-prepare.md "PREPARE"). 

## Parameters

`PREPARE`
    

This key word is ignored. 

_`name`_
    

The name of the prepared statement to deallocate. 

`ALL`
    

Deallocate all prepared statements. 

## Compatibility

The SQL standard includes a `DEALLOCATE` statement, but it is only for use in embedded SQL. 

## See Also

[EXECUTE](sql-execute.md "EXECUTE"), [PREPARE](sql-prepare.md "PREPARE")

* * *

[Prev](sql-createview.md "CREATE VIEW") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-declare.md "DECLARE")  
---|---|---  
CREATE VIEW | [Home](index.md "PostgreSQL 17.5 Documentation")|  DECLARE
