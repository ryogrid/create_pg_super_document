# CopyStreamReceive

## Location
src/bin/pg_basebackup/receivelog.c: 932 - 985

## Overview
CopyStreamReceive receives CopyData messages from a PostgreSQL XLOG stream with timeout support, managing the message buffer and handling various connection states.

## Definition


## Detailed Description
This function is responsible for receiving streaming replication data from a PostgreSQL server via the COPY protocol. It implements a non-blocking receive mechanism that attempts to get data immediately, and if none is available, waits for data up to the specified timeout using CopyStreamPoll. The function manages the message buffer lifecycle, automatically freeing previous buffers and setting up new ones. It distinguishes between different types of completion states: normal timeout, server-initiated end of stream, and actual errors. The function is designed to be called repeatedly in a loop to continuously receive streaming data.

## Parameters / Member Variables
- : PostgreSQL connection object for the streaming replication connection
- : Maximum time to wait for data in milliseconds
- : Optional socket that can interrupt the wait operation
- : Pointer to a char pointer that will be set to point to the received message buffer

## Dependencies
- Functions called/Symbols referenced:
  - [PQfreemem](../P/PQfreemem.md)
  - [PQgetCopyData](../P/PQgetCopyData.md)
  - [CopyStreamPoll](CopyStreamPoll.md)
  - [PQconsumeInput](../P/PQconsumeInput.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - pg_log_error
- Called from (representative examples):
  - [HandleCopyStream](../H/HandleCopyStream.md)

## Notes and Other Information
- Returns the length of received data on success, 0 on timeout/interruption, -1 on error, -2 if server ended the COPY
- The buffer pointer is only valid until the next CopyStreamReceive call
- Automatically frees any previous buffer contents before receiving new data
- Uses a two-stage approach: first attempts immediate receive, then waits if no data is available
- Critical component of pg_basebackup's WAL streaming functionality for continuous replication
- Handles both blocking and non-blocking scenarios gracefully