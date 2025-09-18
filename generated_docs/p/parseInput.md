# parseInput

## Location
src/interfaces/libpq/fe-exec.c: 2020 - 2030

## Overview
Internal function that parses input data from the backend until input is exhausted or a stopping state is reached, without attempting to read more data from the backend.

## Definition


## Detailed Description
The parseInput function serves as a wrapper around the protocol-specific parsing function pqParseInput3. It processes incoming data that has already been read from the backend connection, parsing it according to PostgreSQL's frontend/backend protocol version 3. This function is designed to be called when there is data available to parse but does not perform any I/O operations to fetch additional data from the network.

The function is primarily used internally by libpq to handle asynchronous message processing and maintain the connection state machine. It ensures that available buffered data is processed before the calling function decides whether to wait for more data or return to the application.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection containing buffered input data to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - pqParseInput3
- Called from (representative examples):
  - PQisBusy
  - PQgetResult
  - PQnotifies
  - PQputCopyData

## Notes and Other Information
- This is a static (internal) function not exposed in the public libpq API
- The function does not perform any network I/O operations
- It assumes that input data is already available in the connection's input buffer
- Part of the asynchronous query processing infrastructure in libpq