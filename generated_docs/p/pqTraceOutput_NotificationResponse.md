# pqTraceOutput_NotificationResponse

## Location
[src/interfaces/libpq/fe-trace.c:219-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L219-L227)

## Overview
A static function that parses and outputs the contents of a PostgreSQL NotificationResponse protocol message to the trace log.

## Definition
```c
static void pqTraceOutput_NotificationResponse(FILE *f, const char *message, int *cursor, bool regress)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure and handles the specific parsing and output formatting for NotificationResponse protocol messages. These messages are sent by the PostgreSQL server when a LISTEN/NOTIFY event occurs.

The function parses the NotificationResponse message structure which consists of:
1. Process ID (32-bit integer) of the notifying backend process
2. Channel name (null-terminated string) on which the notification was sent  
3. Payload (null-terminated string) containing the notification message

The function outputs a tab-separated trace line starting with 'NotificationResponse' followed by the parsed fields. The process ID can be suppressed in regression testing mode.

## Parameters / Member Variables
- `f`: FILE pointer to the trace output file where the parsed message will be written
- `message`: Pointer to the raw protocol message buffer containing the NotificationResponse data
- `cursor`: Pointer to the current position in the message buffer; updated as fields are parsed
- `regress`: Boolean flag for regression testing mode - when true, suppresses the actual process ID value

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputInt32](pqTraceOutputInt32.md) (outputs the backend process ID)
  - [pqTraceOutputString](pqTraceOutputString.md) (outputs the channel name)
  - [pqTraceOutputString](pqTraceOutputString.md) (outputs the notification payload)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message dispatching function)

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Part of the protocol message output functions organized by message type
- NotificationResponse messages correspond to PostgreSQL's asynchronous notification system (LISTEN/NOTIFY)
- The regress parameter allows for deterministic output during regression testing by hiding variable process IDs
- Output format is tab-delimited for easy parsing by trace analysis tools
- The function assumes the message buffer contains a properly formatted NotificationResponse according to the PostgreSQL protocol specification