# pq_endmsgread

## Location
[src/backend/libpq/pqcomm.c:1164-1179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1164-L1179)

## Overview
Completes the message reading process by resetting the global message reading state, indicating that a complete message has been successfully read.

## Definition

```c
void
pq_endmsgread(void)
```
## Detailed Description
 is the counterpart to  and must be called after successfully reading a complete message using  and related functions. This function serves as a protocol state cleanup mechanism that marks the end of a message reading operation.

The function performs a simple but critical operation:
1. **State Validation**: Asserts that a message reading operation is currently in progress ( must be true)
2. **State Reset**: Sets the  flag to false, indicating that the message reading operation is complete

This function is automatically called by , but must be explicitly called when using lower-level reading functions like  directly.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - : Global flag indicating whether a message read is in progress
  - : Assertion macro to validate proper state
- Called from (representative examples):
  - : Used during secure connection establishment
  - : Used in replication reply processing
  - : Used during SSL startup processing
  - : Used during backend startup packet processing

## Notes and Other Information
- This is a non-static function, accessible from other modules through libpq.h
- Must be paired with  to properly manage message reading state
- The assertion ensures that the function is only called when a message read is actually in progress
- Failure to call this function after reading a message can leave the communication system in an inconsistent state
- Essential for maintaining proper protocol synchronization between client and server
- Used less frequently than  because  handles this automatically for most use cases

## Simplified Source

```c
// Simplified version of pq_endmsgread
void pq_endmsgread(void) {
    // Verify that we're currently reading a message
    Assert(PqCommReadingMsg);

    // Mark message reading as complete
    PqCommReadingMsg = false;
}
```

Key simplifications made:
- Added descriptive comments explaining each step
- Maintained the essential logic: assertion check and state reset
- This function is already quite simple, so minimal simplification was needed