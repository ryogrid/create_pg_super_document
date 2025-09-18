# dttoasc

## Location
src/interfaces/ecpg/compatlib/informix.c: 656 - 665

## Overview
The dttoasc function converts a timestamp value to its ASCII string representation, providing Informix ESQL/C compatibility for timestamp-to-string conversion operations.

## Definition
```c
int dttoasc(timestamp * ts, char *output)
```

## Detailed Description
This function converts a PostgreSQL timestamp value into a human-readable ASCII string format. It serves as a compatibility wrapper for Informix applications that need to format timestamps as strings. The function uses PostgreSQL's PGTYPEStimestamp_to_asc() internally to perform the actual conversion, then copies the result to the user-provided output buffer.

The function handles the memory management by obtaining a dynamically allocated string from the underlying PostgreSQL function, copying it to the output buffer, and then freeing the temporary memory. This ensures that the caller receives a properly formatted timestamp string without having to manage the intermediate memory allocation.

## Parameters / Member Variables
- `ts`: Pointer to a timestamp value to be converted to ASCII format
- `output`: Character buffer where the ASCII representation of the timestamp will be stored (caller must ensure sufficient space)

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPEStimestamp_to_asc](../P/PGTYPEStimestamp_to_asc.md)
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Located in src/interfaces/ecpg/compatlib/informix.c:656-665
- Always returns 0 (success) - does not perform error checking on parameters
- Part of the Informix compatibility layer in PostgreSQL ECPG
- The caller is responsible for ensuring the output buffer is large enough to hold the formatted timestamp string
- The function performs automatic memory management for the intermediate string conversion
- Uses strcpy() to copy the result, which assumes the output buffer has sufficient space
- The resulting string format follows PostgreSQL's default timestamp representation
- No bounds checking is performed on the output buffer