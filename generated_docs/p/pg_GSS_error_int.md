# pg_GSS_error_int

## Location
[src/interfaces/libpq/fe-gssapi-common.c:26-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-gssapi-common.c#L26-L46)

## Overview
Internal helper function that fetches all GSS-API error messages of a specific type and appends them to a string buffer.

## Definition
```c
static void pg_GSS_error_int(char *s, size_t len, OM_uint32 stat, int type)
```

## Detailed Description
This static function is responsible for extracting and formatting GSS-API error messages. It calls the GSS-API function `gss_display_status` repeatedly to retrieve all available error messages of the specified type (either GSS_CODE for general GSS errors or MECH_CODE for mechanism-specific errors). Multiple error messages are concatenated with spaces between them. The function ensures proper null termination and handles buffer overflow by truncating messages when necessary.

The function uses a message context (`msg_ctx`) to iterate through multiple error messages that might be associated with a single GSS status code. It continues looping until all messages have been retrieved.

## Parameters / Member Variables
- `s`: Character buffer to store the formatted error message(s)
- `len`: Size of the buffer `s`
- `stat`: GSS-API status code containing error information
- `type`: Type of error to retrieve (GSS_CODE or MECH_CODE)

## Dependencies
- Functions called/Symbols referenced:
  - gss_display_status (GSS-API function)
  - gss_release_buffer (GSS-API function)
  - memcpy (standard library)
  - Min (PostgreSQL macro)
  - elog (PostgreSQL logging)
  - gss_buffer_desc (GSS-API type)
  - COMMERROR (PostgreSQL constant)
- Called from (representative examples):
  - [pg_GSS_error](pg_GSS_error.md) (both backend and frontend versions)

## Notes and Other Information
- This is a static function only visible within the compilation unit
- Handles buffer overflow gracefully by truncating and logging a COMMERROR message
- The function is designed to be called twice by `pg_GSS_error`: once for general GSS errors and once for mechanism-specific errors
- Memory management is handled properly with `gss_release_buffer` calls to prevent leaks

## Simplified Source

```c
// Simplified version of pg_GSS_error_int
static void
pg_GSS_error_int(char *s, size_t len, OM_uint32 stat, int type)
{
    gss_buffer_desc gmsg;
    size_t i = 0;
    OM_uint32 msg_ctx = 0;

    // Loop through all available error messages for this status code
    do
    {
        // Get next error message from GSS-API
        if (gss_display_status(&lmin_s, stat, type, GSS_C_NO_OID,
                               &msg_ctx, &gmsg) != GSS_S_COMPLETE)
            break;

        // Add space separator between multiple messages
        if (i > 0 && i < len) {
            s[i] = ' ';
            i++;
        }

        // Copy error message to output buffer (with bounds checking)
        if (i < len) {
            size_t copy_len = Min(len - i, gmsg.length);
            memcpy(s + i, gmsg.value, copy_len);
        }
        i += gmsg.length;

        // Clean up GSS buffer
        gss_release_buffer(&lmin_s, &gmsg);
    }
    while (msg_ctx); // Continue if more messages available

    // Null-terminate the result string
    if (i < len) {
        s[i] = '\0';
    } else {
        // Handle buffer overflow case
        elog(COMMERROR, "incomplete GSS error report");
        s[len - 1] = '\0';
    }
}
```

Key simplifications made:
- Added explanatory comments for each major logic block
- Clarified the loop condition and message context usage
- Made bounds checking logic more explicit with intermediate variable
- Grouped related operations together (separator logic, copying logic, cleanup)
- Preserved all essential error handling and memory management
- Maintained the exact same functional behavior