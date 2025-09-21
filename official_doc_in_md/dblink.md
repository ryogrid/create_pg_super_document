F.11. dblink — connect to other PostgreSQL databases  
---  
[Prev](cube.md "F.10. cube — a multi-dimensional cube data type") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](contrib-dblink-connect.md "dblink_connect")  
  
* * *

## F.11. dblink — connect to other PostgreSQL databases #

[dblink_connect](contrib-dblink-connect.md) — opens a persistent connection to a remote database
[dblink_connect_u](contrib-dblink-connect-u.md) — opens a persistent connection to a remote database, insecurely
[dblink_disconnect](contrib-dblink-disconnect.md) — closes a persistent connection to a remote database
[dblink](contrib-dblink-function.md) — executes a query in a remote database
[dblink_exec](contrib-dblink-exec.md) — executes a command in a remote database
[dblink_open](contrib-dblink-open.md) — opens a cursor in a remote database
[dblink_fetch](contrib-dblink-fetch.md) — returns rows from an open cursor in a remote database
[dblink_close](contrib-dblink-close.md) — closes a cursor in a remote database
[dblink_get_connections](contrib-dblink-get-connections.md) — returns the names of all open named dblink connections
[dblink_error_message](contrib-dblink-error-message.md) — gets last error message on the named connection
[dblink_send_query](contrib-dblink-send-query.md) — sends an async query to a remote database
[dblink_is_busy](contrib-dblink-is-busy.md) — checks if connection is busy with an async query
[dblink_get_notify](contrib-dblink-get-notify.md) — retrieve async notifications on a connection
[dblink_get_result](contrib-dblink-get-result.md) — gets an async query result
[dblink_cancel_query](contrib-dblink-cancel-query.md) — cancels any active query on the named connection
[dblink_get_pkey](contrib-dblink-get-pkey.md) — returns the positions and field names of a relation's primary key fields 
[dblink_build_sql_insert](contrib-dblink-build-sql-insert.md) — builds an INSERT statement using a local tuple, replacing the primary key field values with alternative supplied values 
[dblink_build_sql_delete](contrib-dblink-build-sql-delete.md) — builds a DELETE statement using supplied values for primary key field values 
[dblink_build_sql_update](contrib-dblink-build-sql-update.md) — builds an UPDATE statement using a local tuple, replacing the primary key field values with alternative supplied values 

`dblink` is a module that supports connections to other PostgreSQL databases from within a database session. 

`dblink` can report the following wait events under the wait event type `Extension`. 

`DblinkConnect`
    

Waiting to establish a connection to a remote server. 

`DblinkGetConnect`
    

Waiting to establish a connection to a remote server when it could not be found in the list of already-opened connections. 

`DblinkGetResult`
    

Waiting to receive the results of a query from a remote server. 

See also [postgres_fdw](postgres-fdw.md "F.36. postgres_fdw — access data stored in external PostgreSQL servers"), which provides roughly the same functionality using a more modern and standards-compliant infrastructure. 

* * *

[Prev](cube.md "F.10. cube — a multi-dimensional cube data type") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](contrib-dblink-connect.md "dblink_connect")  
---|---|---  
F.10. cube — a multi-dimensional cube data type | [Home](index.md "PostgreSQL 17.5 Documentation")|  dblink_connect
