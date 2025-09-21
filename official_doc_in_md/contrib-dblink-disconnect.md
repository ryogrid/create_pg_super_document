dblink_disconnect  
---  
[Prev](contrib-dblink-connect-u.md "dblink_connect_u") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")| F.11. dblink — connect to other PostgreSQL databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](contrib-dblink-function.md "dblink")  
  
* * *

## dblink_disconnect

dblink_disconnect — closes a persistent connection to a remote database

## Synopsis
    
    
    dblink_disconnect() returns text
    dblink_disconnect(text connname) returns text
    

## Description

`dblink_disconnect()` closes a connection previously opened by `dblink_connect()`. The form with no arguments closes an unnamed connection. 

## Arguments

 _`connname`_
    

The name of a named connection to be closed. 

## Return Value

Returns status, which is always `OK` (since any error causes the function to throw an error instead of returning). 

## Examples
    
    
    SELECT dblink_disconnect();
     dblink_disconnect
    -------------------
     OK
    (1 row)
    
    SELECT dblink_disconnect('myconn');
     dblink_disconnect
    -------------------
     OK
    (1 row)
    

* * *

[Prev](contrib-dblink-connect-u.md "dblink_connect_u") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")|  [Next](contrib-dblink-function.md "dblink")  
---|---|---  
dblink_connect_u | [Home](index.md "PostgreSQL 17.5 Documentation")|  dblink
