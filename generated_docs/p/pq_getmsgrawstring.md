# pq_getmsgrawstring

## Location
[src/backend/libpq/pqformat.c:608-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L608-L634)

## Overview
Extracts a null-terminated text string from a message buffer without character encoding conversion, returning a direct pointer into the buffer.

## Definition

```c
const char *
pq_getmsgrawstring(StringInfo msg)
```
## Detailed Description
The  function retrieves a null-terminated string from a PostgreSQL message buffer without performing any character encoding conversion. It always returns a pointer directly into the message buffer, making it a zero-copy operation suitable for cases where the string data is already in the correct encoding or when encoding conversion is not desired. The function validates that a proper null terminator exists within the message boundaries and advances the cursor appropriately.

## Parameters / Member Variables
- : A  structure representing the message buffer being read from

## Dependencies
- Functions called/Symbols referenced:
  -  (string length function)
  -  (for error reporting)
  -  (error level constant)
  -  (error code function)
  -  (error code constant)
  -  (error message function)
- Called from (representative examples):
  -  (parallel message handling)
  -  (SASL authentication checking)
  -  (error/notice message parsing)

## Notes and Other Information
- Returns a direct pointer into the message buffer without any data copying or conversion
- Does NOT perform character encoding conversion, unlike
- Automatically determines string length by scanning for null terminator within message boundaries
- Validates that the null terminator exists within the message to prevent reading beyond message boundaries
- Advances the message cursor by string length plus one (to skip the null terminator)
- The StringInfo structure guarantees a trailing null byte, making  safe to use
- More efficient than  when encoding conversion is not needed
- Suitable for protocol elements that are known to be in correct encoding already
- The returned pointer is valid only as long as the message buffer remains unchanged
- Commonly used for internal protocol communication where encoding is controlled
- Used in parallel processing and authentication contexts where raw string access is preferred

## Simplified Source

```c
// Simplified version of pq_getmsgrawstring
const char *pq_getmsgrawstring(StringInfo msg) {
    char *str = &msg->data[msg->cursor];

    // Find string length using strlen (safe due to guaranteed null termination)
    int slen = strlen(str);

    // Validate null terminator is within message bounds
    if (msg->cursor + slen >= msg->len) {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("invalid string in message")));
    }

    // Advance cursor past string and null terminator
    msg->cursor += slen + 1;

    return str;
}
```

Key simplifications made:
- Added explanatory comments for each major operation
- Preserved the boundary validation logic
- Maintained the cursor advancement mechanism
- Kept the error handling for invalid strings
- Emphasized the zero-copy nature by returning direct buffer pointer