# send_repl_origin

## Location
src/backend/replication/pgoutput/pgoutput.c: 2406 - 2432

## Overview
Sends a replication origin message to the logical replication subscriber when origin tracking is enabled and the origin information is available.

## Definition


## Detailed Description
This function handles the transmission of replication origin information in PostgreSQL's logical replication system. Replication origins are used to track the source of changes in multi-node replication scenarios, helping to prevent replication loops and maintain proper change provenance.

The function performs the following operations:
1. **Conditional Sending**: Only processes origin information if the send_origin flag is true
2. **Origin Resolution**: Uses replorigin_by_oid() to resolve the origin ID to a human-readable origin name
3. **Message Formatting**: If the origin name is successfully resolved, prepares and writes a logical replication origin message
4. **Error Handling**: Uses a conservative approach - if the origin name cannot be found, it silently skips sending the origin message rather than throwing an error

The function includes extensive comments discussing alternative behaviors for missing origin names, showing the PostgreSQL developers' consideration of different error handling strategies.

## Parameters / Member Variables
- : LogicalDecodingContext containing output plugin state and connection information
- : RepOriginId identifying the replication origin to send information about
- : XLogRecPtr representing the LSN of the original change at the source
- : Boolean flag indicating whether origin information should be sent

## Dependencies
- Functions called/Symbols referenced:
  - replorigin_by_oid (resolve origin ID to name)
  - OutputPluginWrite (flush pending output)
  - OutputPluginPrepareWrite (prepare for new output)
  - logicalrep_write_origin (write origin message to output stream)
- Called from (representative examples):
  - pgoutput_send_begin (when beginning transaction with origin)
  - pgoutput_begin_prepare_txn (when beginning prepared transaction with origin)
  - pgoutput_stream_start (when starting streaming transaction with origin)

## Notes and Other Information
- Used in multi-master and cascading replication scenarios to track change provenance
- Implements graceful degradation - missing origin names don't break replication
- The function includes detailed comments about alternative error handling strategies
- Origin messages help prevent replication loops by allowing subscribers to identify change sources
- The origin_lsn parameter preserves the original LSN from the source system
- Uses the standard OutputPlugin API for message boundaries and writing
- The conservative error handling approach prioritizes replication stability over strict origin tracking