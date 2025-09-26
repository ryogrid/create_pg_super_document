# pq_beginmessage_reuse

## Location
[src/backend/libpq/pqformat.c:109-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L109-L125)

## Overview
Initializes a StringInfo buffer for sending a PostgreSQL protocol message by reusing an existing buffer, providing better performance by avoiding memory allocation overhead.

## Definition

```c
void
pq_beginmessage_reuse(StringInfo buf, char msgtype)
```
## Detailed Description
This function serves the same purpose as  but is optimized for scenarios where a StringInfo buffer is being reused for multiple messages. Instead of initializing a new buffer with , it uses  to clear the existing buffer contents while preserving the allocated memory. This approach provides better performance by avoiding repeated memory allocation and deallocation cycles.

Like , it stores the message type character in the buffer's cursor field for later use by . The function is particularly useful in high-frequency message sending scenarios where the same buffer can be reused across multiple messages.

## Parameters / Member Variables
- : Pre-existing StringInfo buffer to be reset and reused for message construction (must be allocated in a sufficiently long-lived memory context)
- : Single character identifying the type of PostgreSQL protocol message

## Dependencies
- Functions called/Symbols referenced:
  - [resetStringInfo](../r/resetStringInfo.md) (from StringInfo API)
- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)
  - [printtup](printtup.md)
  - [serializeAnalyzeReceive](../s/serializeAnalyzeReceive.md)
  - [exec_describe_statement_message](../e/exec_describe_statement_message.md)

## Notes and Other Information
- Requires the buffer to be allocated in a sufficiently long-lived memory context since it reuses existing memory
- More efficient than  when sending multiple messages as it avoids repeated memory allocation
- The buffer must have been previously initialized (e.g., with ) before first use
- Commonly used in performance-critical paths where the same buffer is used to send multiple messages in sequence
- The message type storage mechanism is identical to  - stored in the cursor field rather than as message content