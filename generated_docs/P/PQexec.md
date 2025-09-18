# PQexec

## Location
src/interfaces/libpq/fe-exec.c: 2262 - 2275

## Overview
High-level libpq function that sends a query to the backend and returns a complete result, providing a simple synchronous interface for SQL execution.

## Definition


## Detailed Description
PQexec is the most commonly used function in libpq for executing SQL queries. It provides a simple, synchronous interface that combines query submission and result retrieval into a single function call. The function blocks until the query completes and returns a complete PGresult.

Internally, PQexec is implemented as a three-step process: it first calls PQexecStart to prepare the connection for query execution, then PQsendQuery to send the query string to the server, and finally PQexecFinish to wait for and retrieve the complete result.

The function handles error conditions at each step - if PQexecStart fails (indicating the connection is not ready for queries), it returns NULL. If PQsendQuery fails (indicating the query could not be sent), it also returns NULL with an appropriate error message set in conn->errorMessage. Only if both preparatory steps succeed does it proceed to PQexecFinish to retrieve the result.

This function is ideal for simple, synchronous database operations where the application can afford to block until the query completes. For applications requiring non-blocking behavior, the separate PQsendQuery/PQgetResult pattern should be used instead.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection
- : Null-terminated string containing the SQL query to execute

## Dependencies
- Functions called/Symbols referenced:
  - PQexecStart
  - PQsendQuery  
  - PQexecFinish
- Called from (representative examples):
  - PSQLexec
  - SendQuery
  - executeQuery
  - ExecuteSqlStatement
  - run_simple_query

## Notes and Other Information
- Returns NULL if the query could not be sent; check conn->errorMessage for details
- Returns a PGresult pointer if the query was sent, regardless of success or failure
- The caller is responsible for freeing the returned PGresult using PQclear()
- This is a blocking/synchronous function - use PQsendQuery/PQgetResult for non-blocking operation
- Part of the core public libpq API and the most commonly used query execution function
- Suitable for most simple database operations where blocking behavior is acceptable
- The query parameter can contain multiple SQL statements separated by semicolons