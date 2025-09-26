# NotifyMyFrontEnd

## Location
[src/backend/commands/async.c:2233-2256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L2233-L2256)

## Overview
Sends a NOTIFY message to the frontend client, either as a protocol message for remote connections or as a log entry for local output.

## Definition
```c
void NotifyMyFrontEnd(const char *channel, const char *payload, int32 srcPid)
```

## Detailed Description
This function delivers notification messages to the frontend by constructing and sending a NotificationResponse protocol message. It handles two different output destinations: for remote connections, it formats the notification as a proper PostgreSQL protocol message, while for local/server output, it logs the notification as an INFO message.

The function constructs protocol messages using PostgreSQL's message buffer system, including the source process ID, channel name, and payload data. It deliberately does not flush the message immediately, allowing higher-level callers to batch multiple messages together for more efficient network transmission.

## Parameters / Member Variables
- `channel`: Name of the notification channel
- `payload`: Notification payload string (can be empty)
- `srcPid`: Process ID of the backend that generated the notification

## Dependencies
- Functions called/Symbols referenced:
  - whereToSendOutput: Global variable determining output destination
  - [pq_beginmessage](../p/pq_beginmessage.md): Starts construction of protocol message buffer
  - PqMsg_NotificationResponse: Protocol message type constant
  - [pq_sendint32](../p/pq_sendint32.md): Adds 32-bit integer to message buffer
  - [pq_sendstring](../p/pq_sendstring.md): Adds null-terminated string to message buffer  
  - [pq_endmessage](../p/pq_endmessage.md): Completes and queues protocol message
  - elog: Logs message for non-remote output destinations
- Called from:
  - [asyncQueueProcessPageEntries](../a/asyncQueueProcessPageEntries.md): Delivers notifications during queue processing
  - [HandleParallelMessage](../H/HandleParallelMessage.md): Processes notifications in parallel worker contexts

## Notes and Other Information
- Does not perform pq_flush() to allow message batching by callers
- Uses DestRemote check to distinguish between network and local output
- Protocol message format includes srcPid, channel name, and payload in sequence
- For local output, formats as INFO log message showing channel and payload
- Part of PostgreSQL's asynchronous notification system (NOTIFY/LISTEN)
- Function is exposed in async.h header for use by other subsystems