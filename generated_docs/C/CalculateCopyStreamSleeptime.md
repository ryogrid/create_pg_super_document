# CalculateCopyStreamSleeptime

## Location
src/bin/pg_basebackup/receivelog.c: 1236 - 1268

## Overview
CalculateCopyStreamSleeptime computes the appropriate sleep duration for send/receive loops in WAL streaming operations to ensure timely status message transmission while avoiding excessive CPU usage.

## Definition


## Detailed Description
This function calculates how long the streaming loop should sleep before the next iteration, based on standby message timeout requirements. It ensures that status messages are sent to the server within the configured timeout period while maintaining efficient resource usage. The function considers the current time, the configured timeout value, and when the last status message was sent to determine the optimal sleep duration.

When a standby message timeout is configured and streaming is active, the function calculates when the next status message should be sent and determines the sleep time accordingly. If no timeout is configured or streaming has stopped, it returns -1 to indicate indefinite sleep.

## Parameters / Member Variables
- : Current timestamp for calculating time differences
- : Timeout value in seconds for sending status messages to the server
- : Timestamp of when the last status message was sent

## Dependencies
- Functions called/Symbols referenced:
  - feTimestampDifference
- Called from (representative examples):
  - HandleCopyStream

## Notes and Other Information
- This is a static function internal to receivelog.c, used in WAL streaming contexts
- The function ensures a minimum sleep time of 1 second to prevent busy-waiting
- Returns sleep time in milliseconds, or -1 for indefinite sleep
- Relies on the global variable  to determine if streaming is active
- The calculation includes a 1-second buffer (standby_message_timeout - 1) to ensure messages are sent before the actual timeout
- Part of PostgreSQL's replication and backup infrastructure, helping maintain responsive communication with the server
- The sleep time calculation converts timestamp differences to milliseconds for use with system sleep functions