# WalSndUpdateProgress

## Location
src/backend/replication/walsender.c: 1672 - 1687

## Overview
WalSndUpdateProgress is a callback function used by the logical decoding context to track replication lag and send keepalive messages during logical replication.

## Definition


## Detailed Description
WalSndUpdateProgress serves as the 'update_progress' callback for logical decoding in PostgreSQL's WAL sender process. It performs two main functions: tracking replication lag by writing samples to the lag tracker, and sending keepalive messages to prevent timeout when empty transactions are skipped during synchronous replication.

The function implements throttling for lag tracking, recording position samples at most once per second (WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS) and only at transaction end boundaries to avoid flooding the lag tracker during frequent commits. For synchronous replication scenarios where empty transactions are skipped, it proactively sends keepalive messages to prevent downstream timeouts.

## Parameters / Member Variables
- : LogicalDecodingContext containing the current decoding state and transaction information
- : WAL location (XLogRecPtr) representing the current position being processed
- : Transaction ID of the current transaction being processed
- : Boolean indicating whether this transaction was skipped (empty transaction)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (gets current time for lag tracking)
  - [LagTrackerWrite](../L/LagTrackerWrite.md) (records lag samples)
  - SyncRepRequested (checks if synchronous replication is enabled)
  - [WalSndKeepalive](WalSndKeepalive.md) (sends keepalive messages)
  - [ProcessPendingWrites](../P/ProcessPendingWrites.md) (processes pending output)
  - [TimestampDifferenceExceeds](../T/TimestampDifferenceExceeds.md) (time comparison utility)
  - pq_flush_if_writable (network output flushing)
  - pq_is_send_pending (check pending network output)
- Called from (representative examples):
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (as callback during slot creation)
  - [StartLogicalReplication](../S/StartLogicalReplication.md) (as callback during logical replication)

## Notes and Other Information
- Callback function registered with CreateDecodingContext for logical replication progress tracking
- Implements 1-second throttling (WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS = 1000ms) to prevent excessive lag tracker updates
- Only tracks lag at transaction end boundaries since downstream acknowledgments are only available for end-of-transaction LSNs
- Sends keepalive messages during synchronous replication when empty transactions are skipped to prevent timeout
- Handles pending writes and network output flushing to ensure timely communication with standby servers
- Uses static variable sendTime to maintain throttling state across function calls