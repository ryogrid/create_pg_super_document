# socket_putmessage_noblock

## Location
[src/backend/libpq/pqcomm.c:1521-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1521-L1557)

## Overview
A static function that sends a PostgreSQL protocol message without blocking, automatically enlarging the output buffer if necessary to accommodate the entire message.

## Definition

```c
static void
socket_putmessage_noblock(char msgtype, const char *s, size_t len)
```
## Detailed Description
This function is a non-blocking variant of the standard message sending functionality. It guarantees that the message will be successfully placed in the output buffer by automatically expanding the buffer size if needed. The function calculates the total space required for the complete message (including header) and reallocates the send buffer if the current buffer is insufficient.

After ensuring adequate buffer space, it delegates the actual message construction to the standard pq_putmessage function. The function is designed to never fail due to insufficient buffer space, making it suitable for scenarios where message delivery must be guaranteed without blocking the caller.

## Parameters / Member Variables
- : Single character identifying the PostgreSQL protocol message type
- : Pointer to the message body data to be sent
- : Length of the message body data in bytes

Returns:
- : This function does not return a value; it either succeeds or triggers an assertion failure

## Dependencies
- Functions called/Symbols referenced:
  -  (line 1533) - reallocates memory for the send buffer when expansion is needed
  -  (line 1536) - performs the actual message construction and buffering
- Global variables accessed:
  -  - current position in the send buffer
  -  - the actual send buffer memory
  -  - current size of the send buffer

## Notes and Other Information
- This is a static function, only accessible within the pqcomm.c file
- The function calculates required space as: PqSendPointer + 1 (msgtype) + 4 (length) + len (body)
- Uses repalloc() to dynamically resize the buffer, which preserves existing buffer contents
- The Assert(res == 0) ensures that pq_putmessage cannot fail when adequate buffer space is guaranteed
- The PG_USED_FOR_ASSERTS_ONLY macro on the res variable indicates it's only used for debugging assertions
- This function provides a guarantee that messages will be buffered successfully, eliminating buffer overflow concerns
- Suitable for situations where the caller cannot handle or recover from buffer full conditions
- The automatic buffer expansion makes this function slightly more expensive than the regular putmessage variant

## Simplified Source

```c
static void socket_putmessage_noblock(char msgtype, const char *s, size_t len) {
    int res PG_USED_FOR_ASSERTS_ONLY;
    int required;

    // Calculate total space needed: current position + header (1+4) + body
    required = PqSendPointer + 1 + 4 + len;

    // Expand buffer if necessary to guarantee no blocking
    if (required > PqSendBufferSize) {
        PqSendBuffer = repalloc(PqSendBuffer, required);
        PqSendBufferSize = required;
    }

    // Send message (guaranteed to succeed with adequate buffer space)
    res = pq_putmessage(msgtype, s, len);
    Assert(res == 0);
}
```