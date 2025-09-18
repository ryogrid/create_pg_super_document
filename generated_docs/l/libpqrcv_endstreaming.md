# libpqrcv_endstreaming

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 655 - 731

## Overview
Terminates an active WAL streaming session with the primary server, handling the proper protocol shutdown sequence and optionally retrieving the next timeline ID.

## Definition


## Detailed Description
This function implements the proper termination sequence for a WAL streaming connection in PostgreSQL replication. It sends a copy-end message to signal the end of streaming, then processes the server's response which may include information about the next timeline. The function handles multiple possible response scenarios: receiving timeline information in a result set, handling aborted copy operations, and ensuring proper protocol completion.

The function follows PostgreSQL's replication protocol strictly, verifying that all expected response messages are received in the correct order. It performs comprehensive error checking and cleanup, ensuring that the connection is left in a consistent state after streaming termination.

## Parameters / Member Variables
- : Pointer to WalReceiverConn structure containing the active streaming connection
- : Output parameter that receives the next timeline ID reported by the server, or 0 if not reported

## Dependencies
- Functions called/Symbols referenced:
  - PQputCopyEnd
  - PQflush
  - libpqrcv_PQgetResult
  - PQresultStatus
  - PQnfields
  - PQntuples
  - PQgetvalue
  - pg_strtoint32
  - PQendcopy
  - PQclear
  - pchomp
- Called from (representative examples):
  - WalReceiverConn (referenced in streaming termination routines)

## Dependencies
- Functions called/Symbols referenced:
  - PGRES_TUPLES_OK
  - PGRES_COPY_OUT
  - PGRES_COMMAND_OK

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- The function handles multiple possible server response scenarios during streaming termination
- Performs rigorous protocol validation to ensure proper connection state after termination
- The next timeline ID is optional information that may not be provided by all server versions
- Handles both normal termination and error scenarios like mid-stream copy abortion
- Verifies that no unexpected results remain after command completion
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:655-731