SET AUTOCOMMIT  
---  
[Prev](ecpg-sql-prepare.md "PREPARE") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-set-connection.md "SET CONNECTION")  
  
* * *

## SET AUTOCOMMIT

SET AUTOCOMMIT — set the autocommit behavior of the current session

## Synopsis
    
    
    SET AUTOCOMMIT { = | TO } { ON | OFF }
    

## Description

`SET AUTOCOMMIT` sets the autocommit behavior of the current database session. By default, embedded SQL programs are _not_ in autocommit mode, so `COMMIT` needs to be issued explicitly when desired. This command can change the session to autocommit mode, where each individual statement is committed implicitly. 

## Compatibility

`SET AUTOCOMMIT` is an extension of PostgreSQL ECPG. 

* * *

[Prev](ecpg-sql-prepare.md "PREPARE") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-set-connection.md "SET CONNECTION")  
---|---|---  
PREPARE | [Home](index.md "PostgreSQL 17.5 Documentation")|  SET CONNECTION
