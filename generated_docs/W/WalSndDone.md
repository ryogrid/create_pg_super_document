# WalSndDone

## Location
[src/backend/replication/walsender.c:3503-3545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3503-L3545)

## Overview
WalSndDone handles graceful shutdown of WAL sender processes by ensuring all WAL data has been successfully replicated before terminating the connection and exiting.

## Definition

```c
static void
WalSndDone(WalSndSendDataCallback send_data)
```
## Detailed Description
WalSndDone is responsible for the orderly shutdown of WAL sender processes when a shutdown signal has been received from the postmaster. The function implements a careful protocol to ensure data integrity by verifying that all WAL data has been successfully transmitted and acknowledged by the client before terminating.

The shutdown process involves several critical checks:
1. Performs a final send operation to ensure any remaining data is transmitted
2. Determines the replicated position by checking the client's flush location (preferred) or write location (fallback for tools like pg_receivewal)
3. Verifies that the WAL sender has caught up, all sent data has been acknowledged, and no data is pending transmission
4. If all conditions are met, sends a completion message and exits cleanly
5. If not ready to shutdown, sends a keepalive message to maintain the connection

This careful verification prevents data loss during shutdown by ensuring the client has received and processed all transmitted WAL data.

## Parameters / Member Variables
- : Function pointer to the appropriate data sending callback (either XLogSendPhysical or XLogSendLogical depending on replication type)

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid
  - pq_is_send_pending
  - SetQueryCompletion
  - [EndCommand](../E/EndCommand.md)
  - pq_flush
  - [proc_exit](../p/proc_exit.md)
  - [WalSndKeepalive](WalSndKeepalive.md)
- Called from (representative examples):
  - [WalSndLoop](WalSndLoop.md)

## Notes and Other Information
- Should only be called when a shutdown signal has been received from postmaster
- Prioritizes flush location over write location for determining replication progress, accommodating different client behaviors
- Uses QueryCompletion with CMDTAG_COPY to properly signal the end of XLOG streaming to the client
- Implements keepalive mechanism when not ready to shutdown to prevent connection timeouts
- Returns control to caller if more data needs to be sent, allowing the main loop to continue processing
- Ensures proper protocol termination by flushing pending data before process exit
- The function name reflects its role as the final step in WAL sender lifecycle management