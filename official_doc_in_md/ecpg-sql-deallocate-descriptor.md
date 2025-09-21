DEALLOCATE DESCRIPTOR  
---  
[Prev](ecpg-sql-connect.md "CONNECT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-declare.md "DECLARE")  
  
* * *

## DEALLOCATE DESCRIPTOR

DEALLOCATE DESCRIPTOR — deallocate an SQL descriptor area

## Synopsis
    
    
    DEALLOCATE DESCRIPTOR _name_
    

## Description

`DEALLOCATE DESCRIPTOR` deallocates a named SQL descriptor area. 

## Parameters

 _`name`_ #
    

The name of the descriptor which is going to be deallocated. It is case sensitive. This can be an SQL identifier or a host variable. 

## Examples
    
    
    EXEC SQL DEALLOCATE DESCRIPTOR mydesc;
    

## Compatibility

`DEALLOCATE DESCRIPTOR` is specified in the SQL standard. 

## See Also

[ALLOCATE DESCRIPTOR](ecpg-sql-allocate-descriptor.md "ALLOCATE DESCRIPTOR"), [GET DESCRIPTOR](ecpg-sql-get-descriptor.md "GET DESCRIPTOR"), [SET DESCRIPTOR](ecpg-sql-set-descriptor.md "SET DESCRIPTOR")

* * *

[Prev](ecpg-sql-connect.md "CONNECT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-declare.md "DECLARE")  
---|---|---  
CONNECT | [Home](index.md "PostgreSQL 17.5 Documentation")|  DECLARE
