# buildWorkerCommand

## Location
[src/bin/pg_dump/parallel.c:1108-1122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1108-L1122)

## Overview
Formats command strings to send to parallel worker processes, specifying which table of contents entry to dump or restore.

## Definition

```c
static void
buildWorkerCommand(ArchiveHandle *AH, TocEntry *te, T_Action act,
				   char *buf, int buflen)
```
## Detailed Description
buildWorkerCommand constructs text-based command messages that the leader process sends to worker processes through inter-process communication channels. The function creates simple command strings that specify the action type (DUMP or RESTORE) and the dump ID of the table of contents entry to be processed. The command format is standardized across all archive formats, though the function design allows for future format-specific extensions. Commands are written to a caller-provided buffer with bounds checking.

## Parameters / Member Variables
- `*AH`: Archive handle (not currently used but maintained for future extensibility)
- `*te`: Table of contents entry containing the dumpId to be processed
- `act`: Action type specifying whether to DUMP or RESTORE the entry
- `*buf`: Caller-supplied buffer to store the formatted command string
- `buflen`: Size of the buffer to prevent buffer overflows
## Dependencies
- Functions called/Symbols referenced:
  - snprintf (formatted string construction)
  - ACT_DUMP (dump action constant)
  - ACT_RESTORE (restore action constant)
  - Assert (error checking for invalid actions)
- Called from (representative examples):
  - [DispatchJobForTocEntry](../D/DispatchJobForTocEntry.md) (src/bin/pg_dump/parallel.c:1220)

## Notes and Other Information
- [Command](../C/Command.md) format is "DUMP <dumpId>" for dump operations and "RESTORE <dumpId>" for restore operations
- Uses snprintf for safe string formatting with buffer bounds checking
- Function is currently format-agnostic but designed to allow format-specific overrides in the future
- Static function scope limits visibility to the parallel.c module
- Asserts false for any action type other than ACT_DUMP or ACT_RESTORE
- The dumpId serves as a unique identifier for the table of contents entry across the entire backup/restore operation