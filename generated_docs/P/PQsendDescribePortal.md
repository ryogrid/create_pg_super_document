# PQsendDescribePortal

## Location
[src/interfaces/libpq/fe-exec.c:2504-2520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2504-L2520)

## Overview
Submits a Describe Portal command to the server without waiting for completion, enabling asynchronous operation for portal metadata retrieval.

## Definition
int PQsendDescribePortal(PGconn *conn, const char *portal)

## Detailed Description
PQsendDescribePortal is an asynchronous function that sends a Describe message to the PostgreSQL server to retrieve metadata about a previously created portal. Unlike PQdescribePortal, this function does not wait for the server's response, allowing the application to continue processing while the server handles the request.

The function returns immediately after sending the command, and the application must later call PQgetResult() to retrieve the actual result. This is primarily useful with portals created by SQL DECLARE CURSOR commands, as libpq doesn't typically expose portals directly to client applications.

## Parameters / Member Variables
- `conn`: Connection to the PostgreSQL server
- `portal`: Name of the portal to describe

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendTypedCommand](PQsendTypedCommand.md)
  - PqMsg_Describe
- Called from (representative examples):
  - [test_prepared](../t/test_prepared.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:1351)

## Notes and Other Information
- This is the asynchronous version of PQdescribePortal
- Returns 1 if successfully submitted, 0 if error (with conn->errorMessage set)
- The application must call PQgetResult() to retrieve the actual response
- Uses the PostgreSQL protocol Describe message with type 'P' for portals
- Primarily useful with portals created by SQL DECLARE CURSOR commands
- No parameter information is returned since portals are already bound to specific values
- Enables non-blocking operation patterns in client applications
- Part of the asynchronous command interface that allows better application responsiveness