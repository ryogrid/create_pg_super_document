# pq_startmsgread

## Location
[src/backend/libpq/pqcomm.c:1140-1163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1140-L1163)

## Overview
Initiates the process of reading a message from the client by setting the global message reading state and performing protocol synchronization validation.

## Definition

```c
void
pq_startmsgread(void)
```
## Detailed Description
 is a critical function that must be called before any of the  functions can be used to read data from a client connection. It serves as a protocol state management function that ensures proper synchronization between the server and client communication.

The function performs two main operations:
1. **Protocol Violation Check**: Verifies that no message reading operation is already in progress by checking the  flag
2. **State Initialization**: Sets the  flag to true, indicating that a message reading operation has begun

If a read operation is already active when this function is called, it indicates a serious protocol synchronization error, and the function will terminate the connection with a FATAL error.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - : Global flag indicating whether a message read is in progress
  - : Error reporting function (when protocol violation detected)
  - : Error level constant
  - : Error code for protocol violations
- Called from (representative examples):
  - : Used during COPY command data retrieval
  - : Used during SASL authentication process
  - : Used during password authentication
  - : Used during backend startup
  - : Used in main socket communication loop

## Notes and Other Information
- This is a non-static function, accessible from other modules through libpq.h
- Must be paired with  to properly manage message reading state
- Critical for maintaining protocol synchronization between client and server
- Failure to call this function before using  functions can lead to undefined behavior
- The protocol violation check helps detect and prevent communication desynchronization issues
- Used extensively throughout PostgreSQL's authentication, replication, and general communication systems