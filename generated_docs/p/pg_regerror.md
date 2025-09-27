# pg_regerror

## Location
[src/backend/regex/regerror.c:60-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regerror.c#L60-L120)

## Overview
The pg_regerror function provides a standardized interface for converting regex error codes into human-readable error messages and handling bidirectional conversion between error codes and their symbolic names.

## Definition

```c
struct rerr *r;
```
## Detailed Description
pg_regerror serves as PostgreSQL's implementation of the POSIX regerror() function with additional functionality. It handles three distinct operations:

1. **Normal error reporting**: Converts standard regex error codes into descriptive error messages
2. **Name-to-number conversion** (REG_ATOI): Converts symbolic error names to their numeric codes  
3. **Number-to-name conversion** (REG_ITOA): Converts numeric error codes to their symbolic names

The function uses a static table of error mappings (rerrs[]) that contains error codes, symbolic names, and explanatory messages. For unknown error codes, it generates a generic error message with the numeric code. The function safely handles buffer overflow by truncating messages that exceed the provided buffer size.

## Parameters / Member Variables
- : The error code to process, or special values REG_ATOI/REG_ITOA for conversion operations
- : Pointer to the associated regex_t structure (currently unused, reserved for future extensions)
- : Output buffer to store the result string, or input buffer for conversion operations
- : Size of the errbuf buffer in bytes; if 0, no copying is performed but length is still returned

## Dependencies
- Functions called/Symbols referenced:
  - regex_t (regex structure type)
  - rerr (error mapping structure)
  - REG_ATOI (constant for name-to-number conversion)
  - REG_ITOA (constant for number-to-name conversion)
- Called from (representative examples):
  - [regcomp_auth_token](../r/regcomp_auth_token.md)
  - [check_ident_usermap](../c/check_ident_usermap.md)
  - [NIAddAffix](../N/NIAddAffix.md)
  - [RE_compile_and_cache](../R/RE_compile_and_cache.md)
  - [RE_wchar_execute](../R/RE_wchar_execute.md)
  - [regexp_fixed_prefix](../r/regexp_fixed_prefix.md)
  - [replace_text_regexp](../r/replace_text_regexp.md)
  - [test_re_compile](../t/test_re_compile.md)
  - [test_re_execute](../t/test_re_execute.md)

## Notes and Other Information
- Returns the total space needed for the error message (including NUL terminator), following POSIX conventions
- Safely handles buffer overflow situations by truncating output and ensuring NUL termination
- The preg parameter is currently unused but maintained for POSIX compatibility and future extensibility
- Uses sprintf for numeric conversions, which could theoretically overflow the convbuf but the buffer is sized generously
- Part of PostgreSQL's custom regex implementation based on Henry Spencer's regex library
- The error table is built from regex/regerrs.h and includes both standard POSIX error codes and PostgreSQL-specific extensions

## Simplified Source

```c
// Simplified version of pg_regerror
size_t pg_regerror(int errcode, const regex_t *preg, char *errbuf, size_t errbuf_size) {
    const struct rerr *r;
    const char *msg;
    char convbuf[sizeof(unk) + 50];  // Buffer for conversions
    size_t len;
    int icode;

    switch (errcode) {
        case REG_ATOI:  // Convert error name to number
            // Find the error name in the table
            for (r = rerrs; r->code >= 0; r++)
                if (strcmp(r->name, errbuf) == 0)
                    break;
            // Convert code to string (-1 if not found)
            sprintf(convbuf, "%d", r->code);
            msg = convbuf;
            break;

        case REG_ITOA:  // Convert error number to name
            // Parse the number from the buffer
            icode = atoi(errbuf);
            // Find the matching code in the table
            for (r = rerrs; r->code >= 0; r++)
                if (r->code == icode)
                    break;
            if (r->code >= 0)
                msg = r->name;
            else {
                // Unknown code - generate generic name
                sprintf(convbuf, "REG_%u", (unsigned) icode);
                msg = convbuf;
            }
            break;

        default:  // Normal error code - get explanation
            // Find the error code in the table
            for (r = rerrs; r->code >= 0; r++)
                if (r->code == errcode)
                    break;
            if (r->code >= 0)
                msg = r->explain;  // Use explanation message
            else {
                // Unknown error - generate generic message
                sprintf(convbuf, unk, errcode);
                msg = convbuf;
            }
            break;
    }

    // Calculate space needed (including null terminator)
    len = strlen(msg) + 1;

    // Copy to buffer if provided and sized
    if (errbuf_size > 0) {
        if (errbuf_size > len)
            strcpy(errbuf, msg);  // Full copy
        else {
            // Truncate to fit and ensure null termination
            memcpy(errbuf, msg, errbuf_size - 1);
            errbuf[errbuf_size - 1] = '\0';
        }
    }

    return len;  // Return space needed
}
```

Key simplifications made:
- Added clear comments explaining each operation mode
- Grouped related logic sections together
- Explained the buffer handling and truncation logic
- Clarified the purpose of the conversion buffer
- Maintained all safety checks and POSIX compliance
- Preserved the three distinct operation modes (normal, ATOI, ITOA)