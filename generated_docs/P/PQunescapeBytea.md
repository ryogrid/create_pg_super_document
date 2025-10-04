# PQunescapeBytea

## Location
[src/interfaces/libpq/fe-exec.c:4555-4664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4555-L4664)

## Overview
PQunescapeBytea converts escaped string representations of bytea data back into binary format, supporting both hex and traditional octal escape encodings.

## Definition

```c
unsigned char *
PQunescapeBytea(const unsigned char *strtext, size_t *retbuflen)
```
## Detailed Description
PQunescapeBytea is a libpq client library function that performs the reverse operation of PQescapeBytea. It converts null-terminated string representations of bytea data back into binary format. The function handles two encoding formats:

1. **Hex format**: Strings starting with '\x' followed by hexadecimal digit pairs
2. **Traditional escape format**: Strings using octal escape sequences and backslash doubling

For hex format (\x followed by hex digits):
- Each pair of hex digits represents one byte
- Whitespace between hex pairs is silently ignored (compatible with byteain)
- Bad input characters are silently skipped

For traditional escape format, the following transformations are reversed:
- \\ becomes \ (ASCII 92)
- \ooo becomes a byte with octal value ooo (where ooo is a 3-digit octal number)
- \x becomes x (for any other character x not matching above patterns)

The function automatically detects the format by checking if the string starts with '\x'.

## Parameters / Member Variables
- `*strtext`: Pointer to the null-terminated escaped string to be converted
- `*retbuflen`: Output parameter - pointer to size_t where the length of the result buffer will be stored
## Dependencies
- Functions called/Symbols referenced:
  - malloc (memory allocation for result buffer)
  - [get_hex](../g/get_hex.md) (converts hex character to numeric value)
  - realloc (shrinks buffer to optimal size)
  - ISFIRSTOCTDIGIT (macro: checks if character is valid first octal digit 0-3)
  - ISOCTDIGIT (macro: checks if character is valid octal digit 0-7) 
  - OCTVAL (macro: converts octal character to numeric value)
- Called from (representative examples):
  - Referenced in libpq-fe.h header file declarations

## Notes and Other Information
- Returns NULL if input is NULL or memory allocation fails
- The function allocates memory using malloc() - caller must free the result using PQfreemem() or free()
- The buffer is shrunk to the minimum required size using realloc() for memory efficiency
- Handles malformed input gracefully by silently ignoring bad characters in hex mode
- For traditional escape mode, unrecognized escape sequences after '\' are ignored (the character after '\' will be treated as literal data)
- Corner case: A trailing '\' at the end of input is simply discarded
- The returned buffer length does not include a null terminator (this is binary data)
- Uses defensive malloc(1) instead of malloc(0) to avoid unportable behavior with zero-length inputs

## Simplified Source

```c
unsigned char *PQunescapeBytea(const unsigned char *strtext, size_t *retbuflen)
{
    size_t strtextlen, buflen;
    unsigned char *buffer, *tmpbuf;
    size_t i, j;

    if (strtext == NULL)
        return NULL;

    strtextlen = strlen((const char *) strtext);

    // Handle hex format (\x followed by hex digit pairs)
    if (strtext[0] == '\\' && strtext[1] == 'x') {
        const unsigned char *s;
        unsigned char *p;

        buflen = (strtextlen - 2) / 2;
        buffer = (unsigned char *) malloc(buflen > 0 ? buflen : 1);
        if (buffer == NULL)
            return NULL;

        // Parse hex pairs, skipping bad input
        s = strtext + 2;
        p = buffer;
        while (*s) {
            char v1, v2;
            v1 = get_hex(*s++);
            if (!*s || v1 == (char) -1)
                continue;
            v2 = get_hex(*s++);
            if (v2 != (char) -1)
                *p++ = (v1 << 4) | v2;
        }
        buflen = p - buffer;
    }
    // Handle traditional escape format
    else {
        buffer = (unsigned char *) malloc(strtextlen + 1);
        if (buffer == NULL)
            return NULL;

        for (i = j = 0; i < strtextlen;) {
            switch (strtext[i]) {
                case '\\':
                    i++;
                    if (strtext[i] == '\\') {
                        // Double backslash becomes single backslash
                        buffer[j++] = strtext[i++];
                    } else {
                        // Check for octal escape sequence (\ooo)
                        if ((ISFIRSTOCTDIGIT(strtext[i])) &&
                            (ISOCTDIGIT(strtext[i + 1])) &&
                            (ISOCTDIGIT(strtext[i + 2]))) {
                            int byte;
                            byte = OCTVAL(strtext[i++]);
                            byte = (byte << 3) + OCTVAL(strtext[i++]);
                            byte = (byte << 3) + OCTVAL(strtext[i++]);
                            buffer[j++] = byte;
                        }
                        // Unrecognized escape - ignore backslash, process next char normally
                    }
                    break;
                default:
                    // Copy regular character
                    buffer[j++] = strtext[i++];
                    break;
            }
        }
        buflen = j;
    }

    // Shrink buffer to actual size needed
    tmpbuf = realloc(buffer, buflen + 1);
    if (!tmpbuf) {
        free(buffer);
        return NULL;
    }

    *retbuflen = buflen;
    return tmpbuf;
}
```