# parseWorkerCommand

## Location
[src/bin/pg_dump/parallel.c:1123-1155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1123-L1155)

## Overview
Parses command strings received by worker processes from the leader, extracting the action type and target table of contents entry.

## Definition

```c
static void
parseWorkerCommand(ArchiveHandle *AH, TocEntry **te, T_Action *act,
				   const char *msg)
```
## Detailed Description
parseWorkerCommand is the counterpart to buildWorkerCommand, responsible for interpreting command messages that worker processes receive from the leader through inter-process communication. The function parses standardized command strings to extract the action type (DUMP or RESTORE) and the dump ID, then resolves the dump ID to the corresponding table of contents entry. It performs validation to ensure the command format is correct and that the referenced entry exists. Invalid commands result in a fatal error that terminates the worker process.

## Parameters / Member Variables
- : Archive handle used to look up table of contents entries
- : Output parameter that receives a pointer to the target TocEntry
- : Output parameter that receives the parsed action type (ACT_DUMP or ACT_RESTORE)
- : Input command string to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - messageStartsWith (command prefix matching)
  - sscanf (numeric parsing of dump ID)
  - [getTocEntryByDumpId](../g/getTocEntryByDumpId.md) (TOC entry lookup)
  - strlen (string length validation)
  - Assert (validation of parse completeness and entry existence)
  - [pg_fatal](pg_fatal.md) (error handling for invalid commands)
- Called from (representative examples):
  - [WaitForCommands](../W/WaitForCommands.md) (src/bin/pg_dump/parallel.c:1353)

## Notes and Other Information
- Expects commands in format "DUMP <dumpId>" or "RESTORE <dumpId>"
- Uses sscanf with %n format specifier to validate that the entire message was consumed during parsing
- Asserts that the resolved table of contents entry is not NULL, indicating the dump ID is valid
- Static function scope limits visibility to the parallel.c module
- Fatal error termination for unrecognized commands ensures worker process integrity
- The function design mirrors buildWorkerCommand for consistency in the command protocol
- Validates both command format and data integrity before proceeding with the requested operation