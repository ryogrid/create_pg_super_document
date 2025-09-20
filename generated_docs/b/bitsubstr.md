# bitsubstr

## Location
[src/backend/utils/adt/varbit.c:1038-1046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1038-L1046)

## Overview
The bitsubstr function provides substring extraction functionality for bit strings in PostgreSQL, serving as a wrapper that delegates to the internal bitsubstring function with specific parameters.

## Definition

```c
Datum
bitsubstr(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL standard bit string substring operation as specified in SQL draft 6.10 section 9. It extracts a substring from a bit string starting at a 1-based position with a specified length. The function acts as a thin wrapper around the more general bitsubstring function, passing the three required arguments (bit string, start position, length) and a false flag to indicate this is not a no-length variant.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: VarBit pointer - the source bit string
  - Second argument: int32 - the 1-based starting position for extraction
  - Third argument: int32 - the length of the substring to extract

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (extracts VarBit argument)
  - PG_GETARG_INT32 (extracts integer arguments)
  - [bitsubstring](bitsubstring.md) (performs actual substring extraction with false flag)
  - PG_RETURN_VARBIT_P (returns VarBit result)
- Called from:
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function is defined in src/backend/utils/adt/varbit.c at lines 1038-1046
- Implements SQL standard substring functionality with 1-based indexing
- Follows SQL draft 6.10 specification section 9 for bit string operations
- The actual substring logic is implemented in the bitsubstring helper function
- Uses 1-based position numbering as per SQL standard (not 0-based like C arrays)
- Follows PostgreSQL's V1 calling convention for SQL-callable functions