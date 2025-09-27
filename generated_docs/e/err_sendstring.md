# err_sendstring

## Location
[src/backend/utils/error/elog.c:3477-3488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L3477-L3488)

## Overview
A static helper function that safely appends text strings to error reports being built for the client, with special handling for error recursion scenarios.

## Definition

```c
static void
err_sendstring(StringInfo buf, const char *str)
```
## Detailed Description
This function serves as a wrapper around PostgreSQL's string sending functionality for error reporting. It provides a critical safety mechanism for error handling by detecting error recursion scenarios and switching to ASCII-only string transmission when necessary. During normal operation, it delegates to  for full encoding conversion support. However, when the system detects it's in error recursion trouble (potentially due to encoding conversion failures), it falls back to  to avoid further encoding-related errors. This design ensures that error messages can still be transmitted to clients even when the encoding conversion subsystem itself has failed.

## Parameters / Member Variables
- : StringInfo buffer where the string will be appended for transmission to the client
- : Null-terminated C string to be sent, expected to be plain 7-bit ASCII during error recursion scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [in_error_recursion_trouble](../i/in_error_recursion_trouble.md)
  - [pq_send_ascii_string](../p/pq_send_ascii_string.md)  
  - [pq_sendstring](../p/pq_sendstring.md)
- Called from (representative examples):
  - [send_message_to_frontend](../s/send_message_to_frontend.md) (multiple locations throughout error message construction)

## Notes and Other Information
This function is part of PostgreSQL's robust error handling system that prevents error cascades. The error recursion detection mechanism ensures that even if encoding conversion fails during error reporting, the system can still communicate error information to clients using safe ASCII-only transmission. Code that calls this function during error recursion scenarios must ensure the input strings are plain 7-bit ASCII characters to avoid encoding issues.

## Simplified Source

```c
// Simplified version of err_sendstring
static void err_sendstring(StringInfo buf, const char *str) {
    // Check if we're in error recursion trouble
    if (in_error_recursion_trouble()) {
        // Use ASCII-only transmission to avoid encoding issues
        pq_send_ascii_string(buf, str);
    } else {
        // Use normal string transmission with encoding conversion
        pq_sendstring(buf, str);
    }
}
```

Key simplifications made:
- Added clear comments explaining the two code paths
- Clarified the purpose of error recursion detection
- Core logic: Use ASCII transmission during error recursion, normal transmission otherwise