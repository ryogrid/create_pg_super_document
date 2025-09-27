# SocketBackend

## Location
[src/backend/tcop/postgres.c:364-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L364-L491)

## Overview
A function that handles frontend-backend communication by reading and processing PostgreSQL protocol messages from client connections.

## Definition

```c
static int
SocketBackend(StringInfo inBuf)
```
## Detailed Description
The `SocketBackend` function is the core message reception handler for PostgreSQL's client-server communication. It implements the PostgreSQL frontend-backend protocol by reading message type codes from clients and loading the corresponding message body data into a buffer. 

The function first reads a single byte indicating the message type, then validates this type against known PostgreSQL protocol message types. Based on the message type, it sets appropriate size limits and flags (such as `doing_extended_query_message` and `ignore_till_sync`) that control subsequent message processing behavior. The function handles various message types including queries, extended query protocol messages (Parse, Bind, Execute, etc.), copy operations, and connection termination.

The function includes robust error handling for client disconnections and protocol violations. If an unknown message type is received, it treats this as a fatal error since message boundary synchronization may be lost.

## Parameters / Member Variables
- `inBuf`: A StringInfo buffer where the message body data is loaded after the message type is determined and validated.

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_CANCEL_INTERRUPTS() (interrupt control macro)
  - [pq_startmsgread](../p/pq_startmsgread.md)() (protocol message reading)
  - [pq_getbyte](../p/pq_getbyte.md)() (protocol byte reading)
  - [IsTransactionState](../I/IsTransactionState.md)() (transaction state checking)
  - ereport() (error reporting via COMMERROR, DEBUG1, FATAL levels)
  - [pq_getmessage](../p/pq_getmessage.md)() (protocol message body reading)
  - RESUME_CANCEL_INTERRUPTS() (interrupt control macro)
  - Message type constants (PqMsg_Query, PqMsg_Parse, PqMsg_Bind, etc.)
  - Size limit constants (PQ_LARGE_MESSAGE_LIMIT, PQ_SMALL_MESSAGE_LIMIT)
  - Global variables (whereToSendOutput, doing_extended_query_message, ignore_till_sync)

- Called from (representative examples):
  - [ReadCommand](../R/ReadCommand.md) (src/backend/tcop/postgres.c:497)

## Notes and Other Information
- This function is static, meaning it's only accessible within the postgres.c compilation unit
- The function implements PostgreSQL's wire protocol version 3, where all messages have a type byte followed by a length word
- Different message types have different size limits to prevent memory exhaustion from malformed messages
- The function maintains protocol state through global variables like `doing_extended_query_message` and `ignore_till_sync`
- EOF return indicates client disconnection, which triggers different handling based on transaction state
- The function includes special error reporting behavior when clients disconnect unexpectedly during transactions
- Message type validation prevents protocol desynchronization by rejecting unknown message types
- Interrupt handling is carefully managed around message reading operations to ensure proper cleanup on cancellation

## Simplified Source

```c
// Simplified version of SocketBackend
static int SocketBackend(StringInfo inBuf) {
    int message_type;
    int max_message_length;

    // Step 1: Disable interrupts and read message type from client
    HOLD_CANCEL_INTERRUPTS();
    pq_startmsgread();
    message_type = pq_getbyte();

    // Step 2: Handle client disconnection
    if (message_type == EOF) {
        if (IsTransactionState()) {
            ereport(COMMERROR, "unexpected EOF with open transaction");
        } else {
            whereToSendOutput = DestNone;
            ereport(DEBUG1, "unexpected EOF on client connection");
        }
        return message_type;
    }

    // Step 3: Validate message type and set size limits
    switch (message_type) {
        // Large messages: queries, function calls, data operations
        case PqMsg_Query:
        case PqMsg_FunctionCall:
        case PqMsg_Bind:
        case PqMsg_Parse:
        case PqMsg_CopyData:
            max_message_length = PQ_LARGE_MESSAGE_LIMIT;
            break;

        // Small messages: control operations
        case PqMsg_Terminate:
        case PqMsg_Close:
        case PqMsg_Describe:
        case PqMsg_Execute:
        case PqMsg_Flush:
        case PqMsg_Sync:
        case PqMsg_CopyDone:
        case PqMsg_CopyFail:
            max_message_length = PQ_SMALL_MESSAGE_LIMIT;
            break;

        // Invalid message type - protocol violation
        default:
            ereport(FATAL, "invalid frontend message type %d", message_type);
            break;
    }

    // Step 4: Update protocol state flags based on message type
    update_protocol_state_flags(message_type);

    // Step 5: Read the actual message content
    if (pq_getmessage(inBuf, max_message_length)) {
        return EOF; // Error already logged by pq_getmessage
    }

    // Step 6: Re-enable interrupts and return message type
    RESUME_CANCEL_INTERRUPTS();
    return message_type;
}
```

Key simplifications made:
- Consolidated similar switch cases for large and small message types
- Abstracted detailed protocol state flag management into a conceptual helper function
- Simplified error reporting calls to focus on essential information
- Used more descriptive variable names (message_type instead of qtype)
- Removed detailed comments about specific protocol behavior
- Focused on the main execution flow: read type → validate → set limits → read content