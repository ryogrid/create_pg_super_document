# should_output_to_client

## Location
[src/backend/utils/error/elog.c:248-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L248-L275)

## Overview
Determines whether a message of a given error level should be sent to the client process based on the current output destination and client authentication status.

## Definition
```c
static inline bool should_output_to_client(int elevel)
```

## Detailed Description
This policy-setting subroutine determines whether an error/log message should be sent to the client frontend process. It implements several important security and usability considerations: first, it only sends messages to clients when the output destination is set to DestRemote (indicating a remote client connection). Second, it respects authentication state - during authentication, only ERROR and higher severity messages are sent to prevent information disclosure and avoid overwhelming clients that may not handle NOTICE messages properly during the authentication handshake. After authentication, the function honors the client_min_messages setting with a special case for INFO messages which are always sent regardless of the threshold.

## Parameters / Member Variables
- `elevel`: The error/message level to check (e.g., DEBUG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC, LOG, LOG_SERVER_ONLY)

## Dependencies
- Functions called/Symbols referenced:
  - whereToSendOutput (global variable)
  - DestRemote (enumeration constant)
  - LOG_SERVER_ONLY (constant)
  - ClientAuthInProgress (global variable)
  - client_min_messages (global variable)
  - INFO (constant)
  - ERROR (constant)
- Called from (representative examples):
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - [errstart](../e/errstart.md)
  - [pg_re_throw](../p/pg_re_throw.md)

## Notes and Other Information
- This function is declared as `static inline` for performance optimization during frequent error processing
- Security consideration: messages below ERROR level are suppressed during authentication to prevent information leakage
- Special handling for LOG_SERVER_ONLY messages which are never sent to clients regardless of other settings
- INFO messages are treated specially and bypass the client_min_messages threshold, always being sent to authenticated clients
- Part of PostgreSQL's centralized error reporting policy system that separates server logging from client communication
- Returns false when output destination is not DestRemote (local connections, file output, etc.)

## Simplified Source

```c
// Simplified version of should_output_to_client
static inline bool should_output_to_client(int elevel) {
    // Only send messages to remote clients, never send LOG_SERVER_ONLY
    if (whereToSendOutput == DestRemote && elevel != LOG_SERVER_ONLY) {
        // During authentication: only send ERROR and higher severity
        if (ClientAuthInProgress) {
            return (elevel >= ERROR);
        }
        // After authentication: honor client_min_messages, but always send INFO
        else {
            return (elevel >= client_min_messages || elevel == INFO);
        }
    }

    // Not a remote client connection
    return false;
}
```

Key simplifications made:
- Added clear comments explaining the three main logic branches
- Preserved the exact conditional logic structure
- Maintained all security-critical authentication checks
- Kept the special handling for LOG_SERVER_ONLY and INFO messages
- No actual code removal needed - function was already concise and well-structured