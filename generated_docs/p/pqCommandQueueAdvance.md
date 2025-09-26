# pqCommandQueueAdvance

## Location
src/interfaces/libpq/fe-exec.c: 3142 - 3179

## Overview
Removes a completed query from the head of the command queue when all corresponding results have been received, with protocol-specific synchronization handling.

## Definition


## Detailed Description
pqCommandQueueAdvance manages the advancement of PostgreSQL's internal command queue by removing the head element when it's safe to do so. The function implements protocol-specific logic to ensure proper synchronization between sent commands and received results.

For simple query protocol, advancement only occurs when a ReadyForQuery message is received, since simple queries can contain multiple statements that must all complete. For extended query protocol, the function respects SYNC boundaries to maintain proper error recovery semantics, preventing advancement past a SYNC element unless the corresponding SYNC response is received.

When advancing, the function unlinks the head element, updates queue pointers, and recycles the queue entry for reuse.

## Parameters / Member Variables
- : The PostgreSQL connection containing the command queue
- : True if a ReadyForQuery message was received
- : True if a SYNC response was received

## Dependencies
- Functions called/Symbols referenced:
  - pqRecycleCmdQueueEntry
  - PGcmdQueueEntry
  - PGQUERY_SIMPLE
  - PGQUERY_SYNC

- Called from (representative examples):
  - PQgetResult (fe-exec.c)
  - pqParseInput3 (fe-protocol3.c)

## Notes and Other Information
- Only advances for simple queries when ReadyForQuery is received
- Blocks advancement past SYNC elements until corresponding SYNC response arrives
- Automatically resets queue tail pointer when queue becomes empty
- Recycles removed queue entries to avoid memory allocation overhead
- Critical for maintaining command/response synchronization in pipeline mode