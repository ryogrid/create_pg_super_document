# pq_writestring

## Location
[src/include/libpq/pqformat.h:108-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqformat.h#L108-L127)

## Overview
A static inline function that appends a null-terminated string to a StringInfo buffer with automatic character encoding conversion for PostgreSQL's libpq protocol format handling.

## Definition
```c
static inline void pq_writestring(StringInfoData *pg_restrict buf, const char *pg_restrict str)
```

## Detailed Description
The `pq_writestring` function is a string serialization utility that writes a null-terminated text string to a pre-allocated StringInfo buffer with automatic character encoding conversion from server encoding to client encoding. This function is essential for ensuring that string data transmitted over PostgreSQL's protocol is properly encoded for the client's expected character set.

The function performs several key operations: it calculates the string length, converts the string from server encoding to client encoding using `pg_server_to_client`, copies the converted string (including the null terminator) to the buffer, and manages memory cleanup for any temporary conversion buffers. The function assumes sufficient buffer space has been pre-allocated for the string after conversion.

## Parameters / Member Variables
- `buf`: A pointer to a StringInfoData structure representing the output buffer. Must have sufficient pre-allocated space for the string after encoding conversion.
- `str`: A null-terminated string in server encoding to be written to the buffer. The string will be converted to client encoding before writing.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_server_to_client](pg_server_to_client.md) (character encoding conversion function)
  - strlen (standard library function)
  - memcpy (standard library function)
  - [pfree](pfree.md) (PostgreSQL memory management function)
  - Assert (macro)
- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)

## Notes and Other Information
- Automatically handles character encoding conversion from server to client encoding
- The pre-allocated buffer space must account for potential size changes due to encoding conversion
- Includes null terminator in the transmitted data, maintaining string semantics in the protocol
- Properly manages memory by freeing temporary conversion buffers when needed
- Uses `pg_restrict` annotations for performance optimization
- Critical for internationalization support in PostgreSQL's protocol, ensuring proper character encoding handling across different locales and client configurations
- The function assumes the input string is valid and null-terminated, following PostgreSQL's string handling conventions

## Simplified Source

```c
// Simplified version of pq_writestring
static inline void pq_writestring(StringInfoData *buf, const char *str) {
    // Get original string length
    int original_length = strlen(str);

    // Convert from server encoding to client encoding
    char *converted_string = pg_server_to_client(str, original_length);

    // If conversion happened, get new length
    int final_length = original_length;
    if (converted_string != str) {
        final_length = strlen(converted_string);
    }

    // Verify buffer has enough space (including null terminator)
    Assert(buf->len + final_length + 1 <= buf->maxlen);

    // Copy string to buffer (including null terminator)
    memcpy(buf->data + buf->len, converted_string, final_length + 1);

    // Update buffer length
    buf->len += final_length + 1;

    // Clean up temporary conversion buffer if needed
    if (converted_string != str) {
        pfree(converted_string);
    }
}
```

Key simplifications made:
- Added descriptive variable names for clarity
- Separated encoding conversion logic into clear steps
- Added comments explaining each operation
- Removed pg_restrict qualifiers for simplicity
- Focused on core logic: measure, convert, verify space, copy, update length, cleanup