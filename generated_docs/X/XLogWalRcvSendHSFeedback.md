# XLogWalRcvSendHSFeedback

## Location
src/backend/replication/walreceiver.c: 1169 - 1264

## Overview
Sends hot standby feedback messages to the primary server, communicating the oldest transaction ID that the standby still needs to keep alive for query consistency.

## Definition


## Detailed Description
This function implements PostgreSQL's hot standby feedback mechanism, which allows read-only queries on standby servers to influence vacuum operations on the primary server. The function sends feedback messages containing the standby's xmin and catalog_xmin values to prevent the primary from removing tuples that are still needed by running queries on the standby.

The function constructs and sends a hot standby feedback message (message type 'h') that includes:
- Current timestamp
- Oldest regular transaction ID (xmin) and its epoch
- Oldest catalog transaction ID (catalog_xmin) and its epoch

The function respects timing intervals and only sends feedback when necessary, either at regular intervals defined by wal_receiver_status_interval or when immediately requested. It also handles the case where hot standby feedback is disabled by sending a final message to clear any previously set xmin values.

## Parameters / Member Variables
- : Boolean flag indicating whether to send feedback immediately, bypassing the normal timing interval checks

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp
  - WalRcvComputeNextWakeup
  - HotStandbyActive
  - GetReplicationHorizons
  - ReadNextFullTransactionId
  - XidFromFullTransactionId
  - EpochFromFullTransactionId
  - resetStringInfo
  - pq_sendbyte
  - pq_sendint64
  - pq_sendint32
  - walrcv_send
- Called from (representative examples):
  - WalReceiverMain
  - XLogWalRcvFlush

## Notes and Other Information
- The function maintains a static variable  to track whether the primary currently has xmin information from this standby
- Feedback is only sent when Hot Standby is active and accepting connections
- The function handles epoch boundaries correctly when comparing transaction IDs
- When hot standby feedback is disabled, the function sends InvalidTransactionId values to clear any previously communicated xmin values
- The timing of feedback messages is controlled by wal_receiver_status_interval and the WALRCV_WAKEUP_HSFEEDBACK wakeup reason