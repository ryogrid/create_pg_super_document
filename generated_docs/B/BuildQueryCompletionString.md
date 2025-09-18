# BuildQueryCompletionString

## Location
src/backend/tcop/cmdtag.c: 121 - 163

## Overview
Constructs a command completion string containing the command tag name and optionally the number of processed rows, formatted according to PostgreSQL's wire protocol requirements.

## Definition


## Detailed Description
This function builds the command completion string that PostgreSQL sends to clients after executing a command. The string format varies based on the command type and the  parameter. For commands that display row counts (determined by ), the function appends the number of processed rows to the command name.

The function maintains backward compatibility with PostgreSQL versions 11 and earlier by including a special "0" placeholder for INSERT commands, which historically included the OID of the inserted record. This ensures consistent wire protocol behavior across versions.

The function performs several optimizations: it uses  for efficient string copying, assumes ASCII command names (no encoding conversion needed), and includes assertion checks to prevent buffer overruns. The output buffer must be at least  bytes to accommodate the longest possible completion string.

## Parameters / Member Variables
- : Pre-allocated output buffer that must be at least COMPLETION_TAG_BUFSIZE bytes in length
- : Pointer to QueryCompletion structure containing the command tag and number of processed rows
- : Boolean flag - if true, only the command tag name is included; if false, row count may be appended based on command type

## Dependencies
- Functions called/Symbols referenced:
  -  (retrieves command tag name and length)
  -  (checks if command should display row count)
  -  (converts unsigned long long to string)
  -  (efficient memory copying)
  - COMPLETION_TAG_BUFSIZE (buffer size constant)
  - MAXINT8LEN (maximum length for 64-bit integer string)
  - QueryCompletion (struct type)
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