ALLOCATE DESCRIPTOR  
---  
[Prev](ecpg-sql-commands.md "34.14. Embedded SQL Commands") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-connect.md "CONNECT")  
  
* * *

## ALLOCATE DESCRIPTOR

ALLOCATE DESCRIPTOR — allocate an SQL descriptor area

## Synopsis
    
    
    ALLOCATE DESCRIPTOR _name_
    

## Description

`ALLOCATE DESCRIPTOR` allocates a new named SQL descriptor area, which can be used to exchange data between the PostgreSQL server and the host program. 

Descriptor areas should be freed after use using the `DEALLOCATE DESCRIPTOR` command. 

## Parameters

 _`name`_ #
    

A name of SQL descriptor, case sensitive. This can be an SQL identifier or a host variable. 

## Examples
    
    
    EXEC SQL ALLOCATE DESCRIPTOR mydesc;
    

## Compatibility

`ALLOCATE DESCRIPTOR` is specified in the SQL standard. 

## See Also

[DEALLOCATE DESCRIPTOR](ecpg-sql-deallocate-descriptor.md "DEALLOCATE DESCRIPTOR"), [GET DESCRIPTOR](ecpg-sql-get-descriptor.md "GET DESCRIPTOR"), [SET DESCRIPTOR](ecpg-sql-set-descriptor.md "SET DESCRIPTOR")

* * *

[Prev](ecpg-sql-commands.md "34.14. Embedded SQL Commands") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-connect.md "CONNECT")  
---|---|---  
34.14. Embedded SQL Commands | [Home](index.md "PostgreSQL 17.5 Documentation")|  CONNECT
