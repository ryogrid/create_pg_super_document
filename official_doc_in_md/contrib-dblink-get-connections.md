dblink_get_connections  
---  
[Prev](contrib-dblink-close.md "dblink_close") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")| F.11. dblink — connect to other PostgreSQL databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](contrib-dblink-error-message.md "dblink_error_message")  
  
* * *

## dblink_get_connections

dblink_get_connections — returns the names of all open named dblink connections

## Synopsis
    
    
    dblink_get_connections() returns text[]
    

## Description

`dblink_get_connections` returns an array of the names of all open named `dblink` connections. 

## Return Value

Returns a text array of connection names, or NULL if none.

## Examples
    
    
    SELECT dblink_get_connections();
    

* * *

[Prev](contrib-dblink-close.md "dblink_close") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")|  [Next](contrib-dblink-error-message.md "dblink_error_message")  
---|---|---  
dblink_close | [Home](index.md "PostgreSQL 17.5 Documentation")|  dblink_error_message
