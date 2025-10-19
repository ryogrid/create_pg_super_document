# string_to_uuid

## Location
[src/backend/utils/adt/uuid.c:95-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L95-L144)

## Overview
Internal helper function that parses a UUID string representation and converts it to the internal binary format with comprehensive validation and error handling.

## Definition
```c
static void string_to_uuid(const char *source, pg_uuid_t *uuid, Node *escontext)
```

## Detailed Description
The `string_to_uuid` function performs the actual parsing work for UUID input conversion. It accepts flexible UUID string formats including:
- Standard format: 8-4-4-4-12 hexadecimal digits with hyphens
- Optional braces: {xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}  
- Relaxed hyphen placement: allows hyphens after each group of 4 hex digits

The function validates each character, ensures proper hexadecimal format, and converts pairs of hex characters to bytes. It provides detailed error reporting through PostgreSQL's error handling system when invalid input is encountered.

## Parameters / Member Variables
- `source`: Input string containing the UUID representation to parse
- `uuid`: Pointer to `pg_uuid_t` structure where the parsed binary UUID will be stored
- `escontext`: Node context for error handling (allows soft error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - `memcpy` (copying hex character pairs)
  - `isxdigit` (validating hexadecimal characters) 
  - `strtoul` (converting hex strings to unsigned values)
  - `ereturn` (PostgreSQL error reporting macro)
- Constants used:
  - `UUID_LEN` (UUID length in bytes, typically 16)
- Error codes:
  - `ERRCODE_INVALID_TEXT_REPRESENTATION`
- Called from:
  - [uuid_in](../u/uuid_in.md) (primary input function)
  - `uuid_sortsupport_state` (for sort support)

## Notes and Other Information
- This is a static (internal) function, not directly accessible outside uuid.c
- Supports flexible input formats while maintaining strict validation
- Uses soft error reporting when escontext is provided, allowing caller to handle errors gracefully
- Processes exactly 16 bytes (UUID_LEN) of binary data
- Allows optional braces around the entire UUID string
- Hyphen placement is validated but flexible - allows hyphens after every 2 hex digits if positioned correctly
- All validation failures jump to a single error handling point for consistent error reporting

## Simplified Source

```c
static void
string_to_uuid(const char *source, pg_uuid_t *uuid, Node *escontext)
{
    const char *src = source;
    bool braces = false;
    int i;

    // Handle optional opening brace
    if (src[0] == '{') {
        src++;
        braces = true;
    }

    // Parse 16 bytes of UUID data
    for (i = 0; i < UUID_LEN; i++) {
        char str_buf[3];

        // Check for sufficient characters
        if (src[0] == '\0' || src[1] == '\0')
            goto syntax_error;

        // Extract and validate two hex characters
        memcpy(str_buf, src, 2);
        if (!isxdigit((unsigned char) str_buf[0]) ||
            !isxdigit((unsigned char) str_buf[1]))
            goto syntax_error;

        // Convert hex pair to byte
        str_buf[2] = '\0';
        uuid->data[i] = (unsigned char) strtoul(str_buf, NULL, 16);
        src += 2;

        // Skip optional hyphens at standard positions
        if (src[0] == '-' && (i % 2) == 1 && i < UUID_LEN - 1)
            src++;
    }

    // Handle optional closing brace
    if (braces) {
        if (*src != '}')
            goto syntax_error;
        src++;
    }

    // Ensure no trailing characters
    if (*src != '\0')
        goto syntax_error;

    return;

syntax_error:
    ereturn(escontext,,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s: \"%s\"",
                    "uuid", source)));
}
```