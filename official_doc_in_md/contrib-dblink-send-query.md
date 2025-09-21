dblink_send_query  
---  
[Prev](contrib-dblink-error-message.md "dblink_error_message") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")| F.11. dblink — connect to other PostgreSQL databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](contrib-dblink-is-busy.md "dblink_is_busy")  
  
* * *

## dblink_send_query

dblink_send_query — sends an async query to a remote database

## Synopsis
    
    
    dblink_send_query(text connname, text sql) returns int
    

## Description

`dblink_send_query` sends a query to be executed asynchronously, that is, without immediately waiting for the result. There must not be an async query already in progress on the connection. 

After successfully dispatching an async query, completion status can be checked with `dblink_is_busy`, and the results are ultimately collected with `dblink_get_result`. It is also possible to attempt to cancel an active async query using `dblink_cancel_query`. 

## Arguments

 _`connname`_
    

Name of the connection to use. 

_`sql`_
    

The SQL statement that you wish to execute in the remote database, for example `select * from pg_class`. 

## Return Value

Returns 1 if the query was successfully dispatched, 0 otherwise. 

## Examples
    
    
    SELECT dblink_send_query('dtest1', 'SELECT * FROM foo WHERE f1 < 3');
    

* * *

[Prev](contrib-dblink-error-message.md "dblink_error_message") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")|  [Next](contrib-dblink-is-busy.md "dblink_is_busy")  
---|---|---  
dblink_error_message | [Home](index.md "PostgreSQL 17.5 Documentation")|  dblink_is_busy
