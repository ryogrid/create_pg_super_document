dblink_is_busy  
---  
[Prev](contrib-dblink-send-query.md "dblink_send_query") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")| F.11. dblink — connect to other PostgreSQL databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](contrib-dblink-get-notify.md "dblink_get_notify")  
  
* * *

## dblink_is_busy

dblink_is_busy — checks if connection is busy with an async query

## Synopsis
    
    
    dblink_is_busy(text connname) returns int
    

## Description

`dblink_is_busy` tests whether an async query is in progress. 

## Arguments

 _`connname`_
    

Name of the connection to check. 

## Return Value

Returns 1 if connection is busy, 0 if it is not busy. If this function returns 0, it is guaranteed that `dblink_get_result` will not block. 

## Examples
    
    
    SELECT dblink_is_busy('dtest1');
    

* * *

[Prev](contrib-dblink-send-query.md "dblink_send_query") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")|  [Next](contrib-dblink-get-notify.md "dblink_get_notify")  
---|---|---  
dblink_send_query | [Home](index.md "PostgreSQL 17.5 Documentation")|  dblink_get_notify
