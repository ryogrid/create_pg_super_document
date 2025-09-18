# truncate_flags_desc

## Location
src/backend/access/rmgrdesc/heapdesc.c: 54 - 74

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
  - appendStringInfoString
  - appendStringInfoChar
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