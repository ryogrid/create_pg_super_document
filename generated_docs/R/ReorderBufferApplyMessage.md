# ReorderBufferApplyMessage

## Location
src/backend/replication/logical/reorderbuffer.c: 2040 - 2059

## Overview
Helper function for ReorderBufferProcessTXN that applies logical replication messages during transaction processing, handling both streaming and non-streaming modes.

## Definition


## Detailed Description
ReorderBufferApplyMessage is an internal helper function that processes message changes during logical replication. The function determines whether to use streaming or regular message processing based on the streaming parameter and delegates to the appropriate callback function (rb->stream_message or rb->message) configured in the ReorderBuffer. This abstraction allows the same message processing logic to work in both streaming and batch transaction replay scenarios.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance containing callback functions and state
- `txn`: Current transaction being processed  
- `change`: ReorderBufferChange containing the message data to apply
- `streaming`: Boolean flag indicating whether to use streaming message processing

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBuffer (struct type)
  - ReorderBufferTXN (struct type)  
  - ReorderBufferChange (struct type)
  - rb->stream_message (callback function for streaming)
  - rb->message (callback function for regular processing)
- Called from (representative examples):
  - ReorderBufferProcessTXN

## Notes and Other Information
- This is a static inline function, optimized for performance as it's called frequently during transaction replay
- The function serves as an abstraction layer that allows the same message processing code to work in both streaming and non-streaming contexts
- Message data is accessed through change->data.msg structure containing prefix, message_size, and message content
- The LSN (Log Sequence Number) from the change is passed to maintain proper ordering and consistency