# bitsubstr_no_len

## Location
[src/backend/utils/adt/varbit.c:1047-1054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1047-L1054)

## Overview
The bitsubstr_no_len function provides substring extraction functionality for bit strings without specifying a length, extracting from a starting position to the end of the bit string.

## Definition

```c
Datum
bitsubstr_no_len(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements a variant of bit string substring extraction where no explicit length is provided. It extracts a substring from a bit string starting at a specified 1-based position and continuing to the end of the string. The function delegates to the bitsubstring function with a length parameter of -1 and a true flag to indicate this is the no-length variant, which tells the underlying function to extract from the start position to the end of the string.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: VarBit pointer - the source bit string
  - Second argument: int32 - the 1-based starting position for extraction

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (extracts VarBit argument)
  - PG_GETARG_INT32 (extracts integer start position argument)
  - [bitsubstring](bitsubstring.md) (performs actual substring extraction with -1 length and true flag)
  - PG_RETURN_VARBIT_P (returns VarBit result)
- Called from:
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function is defined in src/backend/utils/adt/varbit.c at lines 1047-1054
- Provides the no-length variant of bit string substring operation
- Uses -1 as a special length value to indicate 'extract to end of string'
- The true flag passed to bitsubstring indicates this is the no-length variant
- Uses 1-based position numbering as per SQL standard
- Complements the bitsubstr function which requires an explicit length parameter
- Follows PostgreSQL's V1 calling convention for SQL-callable functions

## Simplified Source

```c
Datum bitsubstr_no_len(PG_FUNCTION_ARGS) {
    // Extract arguments: bit string and start position (1-based)
    VarBit *source = PG_GETARG_VARBIT_P(0);
    int32 start_pos = PG_GETARG_INT32(1);

    // Extract from start position to end (-1 means "to end", true flag indicates no-length variant)
    PG_RETURN_VARBIT_P(bitsubstring(source, start_pos, -1, true));
}
```