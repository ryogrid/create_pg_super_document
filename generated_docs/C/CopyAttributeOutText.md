# CopyAttributeOutText

## Location
[src/backend/commands/copyto.c:987-1139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L987-L1139)

## Overview
CopyAttributeOutText formats a string attribute for text-mode COPY TO output by applying proper escaping and encoding conversion as needed.

## Definition
```c
static void CopyAttributeOutText(CopyToState cstate, const char *string)
```

## Detailed Description
CopyAttributeOutText processes individual string attributes during text-format COPY TO operations by scanning the input string for characters that require escaping and applying appropriate transformations. The function handles encoding conversion when necessary, escapes control characters using C-like notation (\n, \r, \t, etc.), and escapes delimiter characters and backslashes. It uses an optimized approach with two different code paths depending on whether the encoding embeds ASCII characters, allowing for performance optimization in the common case of safe encodings. The function batches output using the DUMPSOFAR macro to minimize overhead from individual character operations.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing formatting configuration and output settings
- `string`: Input string to be formatted and escaped for text output

## Dependencies
- Functions called/Symbols referenced:
  - [pg_server_to_any](../p/pg_server_to_any.md)
  - DUMPSOFAR
  - [CopySendChar](CopySendChar.md)
  - IS_HIGHBIT_SET
  - [pg_encoding_mblen](../p/pg_encoding_mblen.md)
- Called from (representative examples):
  - [DoCopyTo](../D/DoCopyTo.md)
  - [CopyOneRowTo](CopyOneRowTo.md)
  - DR_copy

## Notes and Other Information
The function includes performance optimizations by providing separate code paths for encodings that embed ASCII versus those that don't, avoiding unnecessary multibyte character length calculations in the safe case. Control characters are converted to their C-style escape sequences (\b, \f, \n, \r, \t, \v) for better readability and compatibility with various systems. The function handles delimiter characters and backslashes by prefixing them with backslashes. The DUMPSOFAR macro is used strategically to output accumulated characters in batches, reducing the number of individual send operations and improving performance for longer strings.

## Simplified Source

```c
static void
CopyAttributeOutText(CopyToState cstate, const char *string)
{
    const char *ptr;
    const char *start;
    char c;
    char delimc = cstate->opts.delim[0];

    if (cstate->need_transcoding)
        ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
    else
        ptr = string;

    // Two code paths for performance - one for encodings that embed ASCII,
    // one for safe encodings where we can skip multibyte length calculations
    if (cstate->encoding_embeds_ascii)
    {
        start = ptr;
        while ((c = *ptr) != '\0')
        {
            if ((unsigned char) c < (unsigned char) 0x20)
            {
                // Escape control characters using C-like notation
                switch (c)
                {
                    case '\b': c = 'b'; break;
                    case '\f': c = 'f'; break;
                    case '\n': c = 'n'; break;
                    case '\r': c = 'r'; break;
                    case '\t': c = 't'; break;
                    case '\v': c = 'v'; break;
                    default:
                        if (c == delimc)
                            break;
                        ptr++;
                        continue;
                }
                // Convert the control char
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                CopySendChar(cstate, c);
                start = ++ptr;
            }
            else if (c == '\\' || c == delimc)
            {
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                start = ptr++;
            }
            else if (IS_HIGHBIT_SET(c))
                ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
            else
                ptr++;
        }
    }
    else
    {
        // Safe encoding path - similar logic but without multibyte handling
        start = ptr;
        while ((c = *ptr) != '\0')
        {
            if ((unsigned char) c < (unsigned char) 0x20)
            {
                // Same control character handling as above
                switch (c)
                {
                    case '\b': c = 'b'; break;
                    case '\f': c = 'f'; break;
                    case '\n': c = 'n'; break;
                    case '\r': c = 'r'; break;
                    case '\t': c = 't'; break;
                    case '\v': c = 'v'; break;
                    default:
                        if (c == delimc)
                            break;
                        ptr++;
                        continue;
                }
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                CopySendChar(cstate, c);
                start = ++ptr;
            }
            else if (c == '\\' || c == delimc)
            {
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                start = ptr++;
            }
            else
                ptr++;
        }
    }

    DUMPSOFAR();
}
```