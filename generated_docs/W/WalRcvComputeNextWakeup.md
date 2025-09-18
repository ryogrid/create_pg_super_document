# WalRcvComputeNextWakeup

## Location
[src/backend/replication/walreceiver.c:1317-1357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L1317-L1357)

## Overview
Computes the next scheduled wakeup time for various WAL receiver operations based on configuration parameters and wakeup reasons.

## Definition


## Detailed Description
This function manages the timing of various periodic operations performed by the WAL receiver process. It calculates when the WAL receiver should next wake up to perform specific actions like sending status replies, hot standby feedback, pings, or handling timeouts.

The function uses a switch statement to handle different wakeup reasons, each with its own timing logic based on relevant GUC (Grand Unified Configuration) parameters. For efficiency, the caller provides the current timestamp to avoid multiple calls to GetCurrentTimestamp().

The calculated wakeup times are stored in a global wakeup array indexed by the wakeup reason, allowing the main WAL receiver loop to efficiently determine when to perform each type of operation.

## Parameters / Member Variables
- : WalRcvWakeupReason enum value indicating which type of wakeup time to compute (TERMINATE, PING, HSFEEDBACK, or REPLY)
- : TimestampTz representing the current time, provided by the caller to avoid redundant timestamp calls

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTzPlusMilliseconds
  - TimestampTzPlusSeconds
  - TIMESTAMP_INFINITY
  - WalRcvWakeupReason enum values (WALRCV_WAKEUP_TERMINATE, WALRCV_WAKEUP_PING, WALRCV_WAKEUP_HSFEEDBACK, WALRCV_WAKEUP_REPLY)
- Called from (representative examples):
  - [WalReceiverMain](WalReceiverMain.md)
  - [XLogWalRcvSendReply](../X/XLogWalRcvSendReply.md)
  - [XLogWalRcvSendHSFeedback](../X/XLogWalRcvSendHSFeedback.md)

## Notes and Other Information
- TERMINATE and PING wakeups are based on wal_receiver_timeout (in milliseconds)
- PING wakeup is scheduled at half the timeout interval to provide early warning
- HSFEEDBACK wakeup is based on wal_receiver_status_interval and requires hot_standby_feedback to be enabled
- REPLY wakeup is based on wal_receiver_status_interval for periodic status updates
- When timeouts or intervals are disabled (≤ 0), wakeup times are set to TIMESTAMP_INFINITY (never)
- The function intentionally has no default case to ensure all wakeup reasons are explicitly handled
- Wakeup times can be recomputed when GUC parameters change during runtime