# pq_getmsgstring

## Location
[src/backend/libpq/pqformat.c:579-607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L579-L607)

## Overview
Extracts a null-terminated text string from a message buffer with optional character encoding conversion, returning either a direct pointer or a converted copy.

## Definition

```c
const char *
pq_getmsgstring(StringInfo msg)
```
## Detailed Description
The  function retrieves a null-terminated string from a PostgreSQL message buffer. It automatically detects the string length by scanning for the null terminator and performs character encoding conversion from client to server encoding if necessary. The function may return either a pointer directly into the message buffer (if no conversion is needed) or a pointer to a freshly allocated converted result. The function validates that a proper null terminator exists within the message boundaries.

## Parameters / Member Variables
- `msg`: A  structure representing the message buffer being read from
## Dependencies
- Functions called/Symbols referenced:
  -  (string length function)
  -  (for error reporting)
  -  (error level constant)
  -  (error code function)
  -  (error code constant)
  -  (error message function)
  -  (character encoding conversion function)
- Called from (representative examples):
  -  (COPY command data retrieval)
  -  (logical replication begin prepare)
  -  (logical replication prepare common)
  -  (logical replication commit prepared)
  -  (logical replication rollback prepared)
  -  (logical replication origin)
  -  (logical replication relation)
  -  (logical replication type)
  -  (upload manifest packet handling)
  -  (bind message execution)
  -  (main PostgreSQL backend entry point)
  -  (tsquery type receive function)
  -  (tsvector type receive function)

## Notes and Other Information
- Automatically determines string length by scanning for null terminator within message boundaries
- May return a direct pointer into the message buffer or a newly allocated converted string
- Performs character encoding conversion from client to server encoding when necessary
- Validates that the null terminator exists within the message to prevent reading beyond message boundaries
- Advances the message cursor by string length plus one (to skip the null terminator)
- The StringInfo structure guarantees a trailing null byte, making  safe to use
- Commonly used in protocol message parsing where strings are null-terminated
- Memory management depends on whether conversion occurs - direct pointers require no cleanup, converted strings are managed by PostgreSQL's memory context system
- Extensively used in logical replication protocol message parsing

## Simplified Source

```c
// Simplified version of pq_getmsgstring
const char *
pq_getmsgstring(StringInfo msg) {
    // Get pointer to current position in message buffer
    char *str = &msg->data[msg->cursor];

    // Find string length using strlen (safe because StringInfo has trailing null)
    int string_length = strlen(str);

    // Validate that null terminator is within message bounds
    if (msg->cursor + string_length >= msg->len) {
        // Report protocol violation error
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("invalid string in message")));
    }

    // Move cursor past the string and its null terminator
    msg->cursor += string_length + 1;

    // Convert from client encoding to server encoding and return
    return pg_client_to_server(str, string_length);
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Used more descriptive variable name (`string_length` instead of `slen`)
- Simplified the error reporting structure for clarity
- Focused on the main execution path
- Preserved all essential functionality and error checking