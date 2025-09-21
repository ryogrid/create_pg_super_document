SET CONNECTION  
---  
[Prev](ecpg-sql-set-autocommit.md "SET AUTOCOMMIT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-set-descriptor.md "SET DESCRIPTOR")  
  
* * *

## SET CONNECTION

SET CONNECTION — select a database connection

## Synopsis
    
    
    SET CONNECTION [ TO | = ] _connection_name_
    

## Description

`SET CONNECTION` sets the “current” database connection, which is the one that all commands use unless overridden. 

## Parameters

 _`connection_name`_ #
    

A database connection name established by the `CONNECT` command. 

`CURRENT` #
    

Set the connection to the current connection (thus, nothing happens). 

## Examples
    
    
    EXEC SQL SET CONNECTION TO con2;
    EXEC SQL SET CONNECTION = con1;
    

## Compatibility

`SET CONNECTION` is specified in the SQL standard. 

## See Also

[CONNECT](ecpg-sql-connect.md "CONNECT"), [DISCONNECT](ecpg-sql-disconnect.md "DISCONNECT")

* * *

[Prev](ecpg-sql-set-autocommit.md "SET AUTOCOMMIT") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-set-descriptor.md "SET DESCRIPTOR")  
---|---|---  
SET AUTOCOMMIT | [Home](index.md "PostgreSQL 17.5 Documentation")|  SET DESCRIPTOR
