# truncate_flags_desc

## Location
[src/backend/access/rmgrdesc/heapdesc.c:54-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/heapdesc.c#L54-L74)

## Overview
A static utility function that formats heap truncate operation flags into a human-readable string representation for WAL record descriptions in PostgreSQL debugging and logging.

## Definition
```c
static void truncate_flags_desc(StringInfo buf, uint8 flags)
```

## Detailed Description
The `truncate_flags_desc` function is a formatting utility used within the heap resource manager description system to convert truncate operation flags into readable string format. It examines the provided flags parameter and appends descriptive text to a StringInfo buffer, showing which specific truncate options are enabled for a given operation. The function creates output in the format "flags: [FLAG1, FLAG2, ...]" and handles proper comma and spacing formatting automatically.

This function is specifically used when describing heap truncate WAL records, helping developers and database administrators understand what options were specified during table truncation operations. The formatted output aids in debugging, monitoring, and analyzing WAL record contents during database recovery or troubleshooting scenarios.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted truncate flags description will be appended
- `flags`: uint8 value containing the bitwise flags representing truncate operation options

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - XLH_TRUNCATE_CASCADE (flag constant)
  - XLH_TRUNCATE_RESTART_SEQS (flag constant)
- Called from:
  - [heap_desc](../h/heap_desc.md)

## Notes and Other Information
- Implements the same comma and space management pattern as other description functions in the file
- Only handles two specific truncate flags: CASCADE and RESTART_SEQS
- Used exclusively for WAL record description in heap truncate operations
- The CASCADE flag indicates that dependent objects should also be truncated
- The RESTART_SEQS flag indicates that associated sequences should be restarted from their initial values

## Simplified Source

```c
static void truncate_flags_desc(StringInfo buf, uint8 flags) {
    // Start the flags description
    appendStringInfoString(buf, "flags: [");

    // Check each truncate flag and append description
    if (flags & XLH_TRUNCATE_CASCADE)
        appendStringInfoString(buf, "CASCADE, ");
    if (flags & XLH_TRUNCATE_RESTART_SEQS)
        appendStringInfoString(buf, "RESTART_SEQS, ");

    // Remove trailing ", " if any flags were added
    if (buf->data[buf->len - 1] == ' ') {
        buf->len -= 2;  // Remove ", "
        buf->data[buf->len] = '\0';
    }

    // Close the flags description
    appendStringInfoChar(buf, ']');
}
```