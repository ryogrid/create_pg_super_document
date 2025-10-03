# BuildQueryCompletionString

## Location
[src/backend/tcop/cmdtag.c:121-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/cmdtag.c#L121-L163)

## Overview
Constructs a command completion string containing the command tag name and optionally the number of processed rows, formatted according to PostgreSQL's wire protocol requirements.

## Definition

```c
Size
BuildQueryCompletionString(char *buff, const QueryCompletion *qc,
						   bool nameonly)
```
## Detailed Description
This function builds the command completion string that PostgreSQL sends to clients after executing a command. The string format varies based on the command type and the  parameter. For commands that display row counts (determined by ), the function appends the number of processed rows to the command name.

The function maintains backward compatibility with PostgreSQL versions 11 and earlier by including a special "0" placeholder for INSERT commands, which historically included the OID of the inserted record. This ensures consistent wire protocol behavior across versions.

The function performs several optimizations: it uses  for efficient string copying, assumes ASCII command names (no encoding conversion needed), and includes assertion checks to prevent buffer overruns. The output buffer must be at least  bytes to accommodate the longest possible completion string.

## Parameters / Member Variables
- `*buff`: Pre-allocated output buffer that must be at least COMPLETION_TAG_BUFSIZE bytes in length
- `*qc`: Pointer to QueryCompletion structure containing the command tag and number of processed rows
- `nameonly`: Boolean flag - if true, only the command tag name is included; if false, row count may be appended based on command type
## Dependencies
- Functions called/Symbols referenced:
  -  (retrieves command tag name and length)
  -  (checks if command should display row count)
  -  (converts unsigned long long to string)
  -  (efficient memory copying)
  - COMPLETION_TAG_BUFSIZE (buffer size constant)
  - MAXINT8LEN (maximum length for 64-bit integer string)
  - [QueryCompletion](../Q/QueryCompletion.md) (struct type)
  - CommandTag (enum type)
- Called from (representative examples):
  -  (src/backend/tcop/dest.c:180)
  -  (src/include/tcop/cmdtag.h:59)

## Notes and Other Information
- Returns the length of the constructed string (excluding null terminator)
- For INSERT commands specifically, includes a "0" placeholder for backward compatibility with OID-based completion tags
- Assumes command tag names are plain ASCII and require no encoding conversion
- Uses assertion checks to prevent buffer overflows and ensure string length consistency
- Critical for client-server communication as completion strings inform clients about command execution results
- The function is designed for high performance with minimal memory allocation and efficient string operations

## Simplified Source

```c
// Simplified version of BuildQueryCompletionString
Size BuildQueryCompletionString(char *buff, const QueryCompletion *qc, bool nameonly) {
    CommandTag tag = qc->commandTag;
    Size taglen;
    const char *tagname = GetCommandTagNameAndLen(tag, &taglen);

    // Copy command tag name to buffer
    memcpy(buff, tagname, taglen);
    char *bufp = buff + taglen;

    // Add row count for applicable commands (unless name-only requested)
    if (command_tag_display_rowcount(tag) && !nameonly) {
        // Special case: INSERT needs "0" for backward compatibility
        if (tag == CMDTAG_INSERT) {
            *bufp++ = ' ';
            *bufp++ = '0';
        }

        // Append the actual row count
        *bufp++ = ' ';
        bufp += pg_ulltoa_n(qc->nprocessed, bufp);
    }

    // Null-terminate the string
    *bufp = '\0';

    return bufp - buff;
}
```

Key simplifications made:
- Removed detailed comments and assertions for clarity
- Consolidated buffer pointer management
- Simplified the INSERT special case handling
- Focused on the main logic flow: copy tag name, optionally add counts, terminate string
- Preserved essential backward compatibility logic