dblink_cancel_query  
---  
[Prev](contrib-dblink-get-result.md "dblink_get_result") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")| F.11. dblink — connect to other PostgreSQL databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](contrib-dblink-get-pkey.md "dblink_get_pkey")  
  
* * *

## dblink_cancel_query

dblink_cancel_query — cancels any active query on the named connection

## Synopsis
    
    
    dblink_cancel_query(text connname) returns text
    

## Description

`dblink_cancel_query` attempts to cancel any query that is in progress on the named connection. Note that this is not certain to succeed (since, for example, the remote query might already have finished). A cancel request simply improves the odds that the query will fail soon. You must still complete the normal query protocol, for example by calling `dblink_get_result`. 

## Arguments

 _`connname`_
    

Name of the connection to use. 

## Return Value

Returns `OK` if the cancel request has been sent, or the text of an error message on failure. 

## Examples
    
    
    SELECT dblink_cancel_query('dtest1');
    

* * *

[Prev](contrib-dblink-get-result.md "dblink_get_result") | [Up](dblink.md "F.11. dblink — connect to other PostgreSQL databases")|  [Next](contrib-dblink-get-pkey.md "dblink_get_pkey")  
---|---|---  
dblink_get_result | [Home](index.md "PostgreSQL 17.5 Documentation")|  dblink_get_pkey
