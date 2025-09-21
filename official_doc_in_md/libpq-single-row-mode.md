32.6. Retrieving Query Results in Chunks  
---  
[Prev](libpq-pipeline-mode.md "32.5. Pipeline Mode") | [Up](libpq.md "Chapter 32. libpq — C Library")| Chapter 32. libpq — C Library| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](libpq-cancel.md "32.7. Canceling Queries in Progress")  
  
* * *

## 32.6. Retrieving Query Results in Chunks #

Ordinarily, libpq collects an SQL command's entire result and returns it to the application as a single `PGresult`. This can be unworkable for commands that return a large number of rows. For such cases, applications can use [`PQsendQuery`](libpq-async.md#LIBPQ-PQSENDQUERY) and [`PQgetResult`](libpq-async.md#LIBPQ-PQGETRESULT) in _single-row mode_ or _chunked mode_. In these modes, result row(s) are returned to the application as they are received from the server, one at a time for single-row mode or in groups for chunked mode. 

To enter one of these modes, call [`PQsetSingleRowMode`](libpq-single-row-mode.md#LIBPQ-PQSETSINGLEROWMODE) or [`PQsetChunkedRowsMode`](libpq-single-row-mode.md#LIBPQ-PQSETCHUNKEDROWSMODE) immediately after a successful call of [`PQsendQuery`](libpq-async.md#LIBPQ-PQSENDQUERY) (or a sibling function). This mode selection is effective only for the currently executing query. Then call [`PQgetResult`](libpq-async.md#LIBPQ-PQGETRESULT) repeatedly, until it returns null, as documented in [Section 32.4](libpq-async.md "32.4. Asynchronous Command Processing"). If the query returns any rows, they are returned as one or more `PGresult` objects, which look like normal query results except for having status code `PGRES_SINGLE_TUPLE` for single-row mode or `PGRES_TUPLES_CHUNK` for chunked mode, instead of `PGRES_TUPLES_OK`. There is exactly one result row in each `PGRES_SINGLE_TUPLE` object, while a `PGRES_TUPLES_CHUNK` object contains at least one row but not more than the specified number of rows per chunk. After the last row, or immediately if the query returns zero rows, a zero-row object with status `PGRES_TUPLES_OK` is returned; this is the signal that no more rows will arrive. (But note that it is still necessary to continue calling [`PQgetResult`](libpq-async.md#LIBPQ-PQGETRESULT) until it returns null.) All of these `PGresult` objects will contain the same row description data (column names, types, etc.) that an ordinary `PGresult` object for the query would have. Each object should be freed with [`PQclear`](libpq-exec.md#LIBPQ-PQCLEAR) as usual. 

When using pipeline mode, single-row or chunked mode needs to be activated for each query in the pipeline before retrieving results for that query with `PQgetResult`. See [Section 32.5](libpq-pipeline-mode.md "32.5. Pipeline Mode") for more information. 

`PQsetSingleRowMode` #
    

Select single-row mode for the currently-executing query. 
    
    
    int PQsetSingleRowMode(PGconn *conn);
    

This function can only be called immediately after [`PQsendQuery`](libpq-async.md#LIBPQ-PQSENDQUERY) or one of its sibling functions, before any other operation on the connection such as [`PQconsumeInput` ](libpq-async.md#LIBPQ-PQCONSUMEINPUT) or [`PQgetResult`](libpq-async.md#LIBPQ-PQGETRESULT). If called at the correct time, the function activates single-row mode for the current query and returns 1. Otherwise the mode stays unchanged and the function returns 0. In any case, the mode reverts to normal after completion of the current query. 

`PQsetChunkedRowsMode` #
    

Select chunked mode for the currently-executing query. 
    
    
    int PQsetChunkedRowsMode(PGconn *conn, int chunkSize);
    

This function is similar to [`PQsetSingleRowMode`](libpq-single-row-mode.md#LIBPQ-PQSETSINGLEROWMODE), except that it specifies retrieval of up to _`chunkSize`_ rows per `PGresult`, not necessarily just one row. This function can only be called immediately after [`PQsendQuery`](libpq-async.md#LIBPQ-PQSENDQUERY) or one of its sibling functions, before any other operation on the connection such as [`PQconsumeInput` ](libpq-async.md#LIBPQ-PQCONSUMEINPUT) or [`PQgetResult`](libpq-async.md#LIBPQ-PQGETRESULT). If called at the correct time, the function activates chunked mode for the current query and returns 1. Otherwise the mode stays unchanged and the function returns 0. In any case, the mode reverts to normal after completion of the current query. 

### Caution

While processing a query, the server may return some rows and then encounter an error, causing the query to be aborted. Ordinarily, libpq discards any such rows and reports only the error. But in single-row or chunked mode, some rows may have already been returned to the application. Hence, the application will see some `PGRES_SINGLE_TUPLE` or `PGRES_TUPLES_CHUNK` `PGresult` objects followed by a `PGRES_FATAL_ERROR` object. For proper transactional behavior, the application must be designed to discard or undo whatever has been done with the previously-processed rows, if the query ultimately fails. 

* * *

[Prev](libpq-pipeline-mode.md "32.5. Pipeline Mode") | [Up](libpq.md "Chapter 32. libpq — C Library")|  [Next](libpq-cancel.md "32.7. Canceling Queries in Progress")  
---|---|---  
32.5. Pipeline Mode | [Home](index.md "PostgreSQL 17.5 Documentation")|  32.7. Canceling Queries in Progress
