dblink_get_notify  
---  
[Prev](contrib-dblink-is-busy.md "dblink_is_busy") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")| F.11. dblink — connect to other PostgreSQL databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](contrib-dblink-get-result.md "dblink_get_result")  
  
* * *

## dblink_get_notify

dblink_get_notify — retrieve async notifications on a connection

## Synopsis
    
    
    dblink_get_notify() returns setof (notify_name text, be_pid int, extra text)
    dblink_get_notify(text connname) returns setof (notify_name text, be_pid int, extra text)
    

## Description

`dblink_get_notify` retrieves notifications on either the unnamed connection, or on a named connection if specified. To receive notifications via dblink, `LISTEN` must first be issued, using `dblink_exec`. For details see [LISTEN](sql-listen.md "LISTEN") and [NOTIFY](sql-notify.md "NOTIFY"). 

## Arguments

 _`connname`_
    

The name of a named connection to get notifications on. 

## Return Value

Returns `setof (notify_name text, be_pid int, extra text)`, or an empty set if none.

## Examples
    
    
    SELECT dblink_exec('LISTEN virtual');
     dblink_exec
    -------------
     LISTEN
    (1 row)
    
    SELECT * FROM dblink_get_notify();
     notify_name | be_pid | extra
    -------------+--------+-------
    (0 rows)
    
    NOTIFY virtual;
    NOTIFY
    
    SELECT * FROM dblink_get_notify();
     notify_name | be_pid | extra
    -------------+--------+-------
     virtual     |   1229 |
    (1 row)
    

* * *

[Prev](contrib-dblink-is-busy.md "dblink_is_busy") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")|  [Next](contrib-dblink-get-result.md "dblink_get_result")  
---|---|---  
dblink_is_busy | [Home](index.md "PostgreSQL 17.5 Documentation")|  dblink_get_result
