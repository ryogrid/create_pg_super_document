# PQdescribePortal

## Location
src/interfaces/libpq/fe-exec.c: 2474 - 2490

## Overview
Obtains information about a previously created portal by sending a Describe message to the PostgreSQL server and waiting for the response.

## Definition
PGresult *PQdescribePortal(PGconn *conn, const char *portal)

## Detailed Description
PQdescribePortal is a synchronous function that retrieves metadata about a previously created portal. It sends a Describe message to the server with portal type 'P' and waits for the complete response. Unlike PQdescribePrepared, this function does not return parameter information since portals are already bound to specific parameter values.

This function is primarily useful with portals created by SQL DECLARE CURSOR commands, as libpq doesn't typically expose portals directly to client applications. The function returns a PGresult containing information about the portal's output columns but not input parameters.

## Parameters / Member Variables
- `conn`: Connection to the PostgreSQL server
- `portal`: Name of the portal to describe

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecStart](PQexecStart.md)
  - [PQsendTypedCommand](PQsendTypedCommand.md)
  - PqMsg_Describe
  - [PQexecFinish](PQexecFinish.md)
- Called from (representative examples):
  - [test_prepared](../t/test_prepared.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:1396)

## Notes and Other Information
- This is similar to PQdescribePrepared but for portals instead of prepared statements
- No parameter information is returned since portals are already bound to specific values
- Primarily useful with portals created by SQL DECLARE CURSOR commands
- libpq doesn't typically expose portals directly to client applications
- The caller is responsible for freeing the returned PGresult via PQclear()
- Uses the PostgreSQL protocol Describe message with type 'P' for portals
- Returns NULL if the connection is not in a valid state for sending queries