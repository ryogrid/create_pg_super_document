# PQsendDescribePrepared

## Location
src/interfaces/libpq/fe-exec.c: 2491 - 2503

## Overview
Submits a Describe Statement command for a prepared statement to the server without waiting for completion, enabling asynchronous operation.

## Definition
int PQsendDescribePrepared(PGconn *conn, const char *stmt)

## Detailed Description
PQsendDescribePrepared is an asynchronous function that sends a Describe message to the PostgreSQL server to retrieve metadata about a previously prepared statement. Unlike PQdescribePrepared, this function does not wait for the server's response, allowing the application to continue processing while the server handles the request.

The function returns immediately after sending the command, and the application must later call PQgetResult() to retrieve the actual result. This pattern is useful for applications that need to maintain responsiveness or handle multiple concurrent operations.

## Parameters / Member Variables
- `conn`: Connection to the PostgreSQL server
- `stmt`: Name of the prepared statement to describe

## Dependencies
- Functions called/Symbols referenced:
  - PQsendTypedCommand
  - PqMsg_Describe
- Called from (representative examples):
  - test_prepared (src/test/modules/libpq_pipeline/libpq_pipeline.c:1273)

## Notes and Other Information
- This is the asynchronous version of PQdescribePrepared
- Returns 1 if successfully submitted, 0 if error (with conn->errorMessage set)
- The application must call PQgetResult() to retrieve the actual response
- Uses the PostgreSQL protocol Describe message with type 'S' for prepared statements
- Enables non-blocking operation patterns in client applications
- Part of the asynchronous command interface that allows better application responsiveness