# bytea_substring

## Location
[src/backend/utils/adt/varlena.c:3028-3094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3028-L3094)

## Overview
bytea_substring is a static internal function that performs the core substring extraction logic for bytea values, handling various edge cases and SQL standard compliance.

## Definition

```c
static bytea *
bytea_substring(Datum str,
				int S,
				int L,
				bool length_not_specified)
```
## Detailed Description
This function implements the complete substring extraction logic for bytea values, handling all the complex edge cases required by the SQL99 standard. It processes starting position and length parameters, validates them according to SQL rules, and handles special cases like negative positions, overflow conditions, and unspecified lengths. The function serves as the core implementation used by both bytea_substr and bytea_substr_no_len wrapper functions, as well as bytea_overlay operations.

## Parameters / Member Variables
- : Source bytea value as a Datum
- : Starting position (1-based, as per SQL standard)
- : Substring length (-1 if not specified)
- : Boolean flag indicating whether length parameter was provided
- Returns: New bytea containing the extracted substring

## Dependencies
- Functions called/Symbols referenced:
  - Max (macro to get maximum value)
  - ereport (for error reporting)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (to check for integer overflow)
  - PG_STR_GET_BYTEA (to create empty bytea)
  - DatumGetByteaPSlice (performs the actual slice extraction)
- Called from (representative examples):
  - [bytea_substr](bytea_substr.md) (with explicit length)
  - [bytea_substr_no_len](bytea_substr_no_len.md) (without length parameter)
  - [bytea_overlay](bytea_overlay.md) (for overlay operations)

## Notes and Other Information
- This is a static function, only accessible within varlena.c
- Logic generally matches text_substring() for consistency
- Handles SQL99 compliance including error conditions for negative lengths
- Manages integer overflow scenarios when S + L exceeds int32 range
- Converts 1-based SQL positioning to 0-based internal positioning
- Returns zero-length string for invalid position ranges per SQL99
- Uses DatumGetByteaPSlice for the actual memory operations
- Located in src/backend/utils/adt/varlena.c:3028-3094

## Simplified Source

```c
static bytea *
bytea_substring(Datum str, int S, int L, bool length_not_specified)
{
    int32 start_pos;     // Adjusted start position
    int32 length;        // Adjusted substring length
    int32 end_pos;       // End position

    // Ensure start position is at least 1 (SQL standard)
    start_pos = Max(S, 1);

    if (length_not_specified) {
        // No length specified - extract to end of string
        length = -1;
    }
    else if (L < 0) {
        // SQL99 requires error for negative length
        ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR),
                       errmsg("negative substring length not allowed")));
    }
    else if (pg_add_s32_overflow(S, L, &end_pos)) {
        // Handle overflow - extract to end of string
        length = -1;
    }
    else {
        // Normal case - calculate actual length
        if (end_pos < 1)
            return PG_STR_GET_BYTEA("");  // Return empty string

        length = end_pos - start_pos;
    }

    // Convert to 0-based indexing and extract the slice
    return DatumGetByteaPSlice(str, start_pos - 1, length);
}
```

**Key Points:**
- Handles SQL99 compliant substring extraction from bytea values
- Converts 1-based SQL positions to 0-based internal indexing
- Validates parameters and handles edge cases (negative length, overflow)
- Returns empty bytea for invalid ranges, full string if length unspecified