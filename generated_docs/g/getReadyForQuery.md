# getReadyForQuery

## Location
src/interfaces/libpq/fe-protocol3.c: 1610 - 1641

## Overview
Processes ReadyForQuery messages from the PostgreSQL server to update the connection's transaction status state.

## Definition
```c
static int getReadyForQuery(PGconn *conn)
```

## Detailed Description
The getReadyForQuery function handles ReadyForQuery messages which are sent by the PostgreSQL server to indicate that it is ready to process new commands and to communicate the current transaction status. The function reads a single character from the network stream that encodes the transaction state and updates the connection object's xactStatus field accordingly.

This message is typically sent after the completion of a command cycle (after CommandComplete, EmptyQueryResponse, or ErrorResponse messages) to signal that the server is ready for the next command and to inform the client about the current transaction block status.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object that will have its transaction status updated

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetc](../p/pqGetc.md)
  - PQTRANS_IDLE
  - PQTRANS_INTRANS  
  - PQTRANS_INERROR
  - PQTRANS_UNKNOWN
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md) (main message processing loop)
  - [pqFunctionCall3](../p/pqFunctionCall3.md) (function call protocol handling)

## Notes and Other Information
- Returns 0 on success, EOF on failure
- Transaction status character meanings:
  - 'I': Idle (not in a transaction block)
  - 'T': In a transaction block
  - 'E': In a failed transaction block (commands will be rejected until block is ended)
  - Any other character: Unknown status
- Critical for maintaining accurate connection state for transaction management
- Simple but essential function for protocol state tracking