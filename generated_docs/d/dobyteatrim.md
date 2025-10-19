# dobyteatrim

## Location
[src/backend/utils/adt/oracle_compat.c:534-616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L534-L616)

## Overview
The dobyteatrim function is the core implementation that provides byte-level trimming functionality for PostgreSQL's bytea (binary data) trim functions.

## Definition
```c
bytea *dobyteatrim(bytea *string, bytea *set, bool doltrim, bool dortrim)
```

## Detailed Description
dobyteatrim is a helper function that implements the common trimming logic for binary data (bytea type) used by byteatrim, bytealtrim, and byteartrim functions. Unlike the text-based dotrim function, this function operates on raw bytes without concern for character encoding or multibyte sequences. It removes specified bytes from the left side, right side, or both sides of a bytea value based on a set of bytes to be trimmed. The function uses a simple byte-by-byte comparison algorithm.

## Parameters / Member Variables
- `string`: The input bytea value to be trimmed
- `set`: The bytea value containing the set of bytes to remove during trimming
- `doltrim`: Boolean flag to enable trimming from the left (start) of the bytea
- `dortrim`: Boolean flag to enable trimming from the right (end) of the bytea

## Dependencies
- Functions called/Symbols referenced:
  - SET_VARSIZE (set the size of a variable-length PostgreSQL data type)
  - VARDATA (get pointer to the actual data within a variable-length type)
- Called from (representative examples):
  - [byteatrim](../b/byteatrim.md) (bidirectional bytea trimming)
  - [bytealtrim](../b/bytealtrim.md) (left-side bytea trimming)
  - [byteartrim](../b/byteartrim.md) (right-side bytea trimming)

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:534-616
- Operates on raw binary data without character encoding considerations
- Uses simple byte-by-byte comparison, making it more efficient than text trimming for binary data
- Returns the original string unchanged if either string or set is empty
- Allocates new bytea structure for the result and copies the trimmed portion
- Part of PostgreSQL's Oracle compatibility layer for binary data operations

## Simplified Source

```c
bytea *dobyteatrim(bytea *string, bytea *set, bool doltrim, bool dortrim) {
    // Get string data and lengths
    char *str_ptr = VARDATA_ANY(string);
    char *set_ptr = VARDATA_ANY(set);
    int str_len = VARSIZE_ANY_EXHDR(string);
    int set_len = VARSIZE_ANY_EXHDR(set);

    // Return original if either string or set is empty
    if (str_len <= 0 || set_len <= 0)
        return string;

    // Set up pointers for trimming
    char *start = str_ptr;
    char *end = str_ptr + str_len - 1;
    int remaining_len = str_len;

    // Trim from left if requested
    if (doltrim) {
        while (remaining_len > 0) {
            // Check if current byte is in trim set
            bool found_in_set = false;
            for (int i = 0; i < set_len; i++) {
                if (*start == set_ptr[i]) {
                    found_in_set = true;
                    break;
                }
            }
            if (!found_in_set) break;
            start++;
            remaining_len--;
        }
    }

    // Trim from right if requested
    if (dortrim) {
        while (remaining_len > 0) {
            // Check if current byte is in trim set
            bool found_in_set = false;
            for (int i = 0; i < set_len; i++) {
                if (*end == set_ptr[i]) {
                    found_in_set = true;
                    break;
                }
            }
            if (!found_in_set) break;
            end--;
            remaining_len--;
        }
    }

    // Create result bytea and copy trimmed data
    bytea *result = (bytea *) palloc(VARHDRSZ + remaining_len);
    SET_VARSIZE(result, VARHDRSZ + remaining_len);
    memcpy(VARDATA(result), start, remaining_len);

    return result;
}
```