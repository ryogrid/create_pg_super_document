# DecodeNumberField

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1087-1196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1087-L1196)

## Overview
DecodeNumberField interprets a numeric string as a concatenated date or time field, using the context of previously decoded fields to determine the appropriate interpretation.

## Definition

```c
static int
DecodeNumberField(int len, char *str, int fmask,
				  int *tmask, struct tm *tm, fsec_t *fsec, bool *is2digits)
```
## Detailed Description
This function is a core component of PostgreSQL's datetime parsing system that handles numeric fields in datetime strings. It can interpret numeric strings as either concatenated date fields (YYYYMMDD format) or time fields (HHMMSS or HHMM format), depending on the length of the input and what fields have already been parsed.

The function handles several key scenarios:
1. **Fractional seconds**: If the string contains a decimal point, it extracts fractional seconds in microseconds
2. **Concatenated dates**: For strings of 6+ digits when no complete date has been parsed, it interprets them as YYYYMMDD (with 2-digit years supported)
3. **Concatenated times**: For 4 or 6 digit strings when time fields are missing, it interprets them as HHMM or HHMMSS respectively

The function returns DTK tokens for successful parsing or DTERR error codes for failures.

## Parameters / Member Variables
- : Length of the input string
- : Numeric string to be decoded (may be modified during processing)
- : Bitmask indicating which fields have already been parsed
- : Pointer to bitmask that will be updated with newly parsed fields
- : Pointer to pg_tm structure to store parsed date/time components
- : Pointer to store fractional seconds (in microseconds)
- : Pointer to boolean indicating if a 2-digit year was encountered

## Dependencies
- Functions called/Symbols referenced:
  - strchr
  - strtod
  - rint
  - strlen
  - atoi
  - DTK_DATE_M, DTK_TIME_M (field mask constants)
  - DTK_DATE, DTK_TIME (return token constants)
  - DTERR_BAD_FORMAT (error constant)
  - struct pg_tm
  - fsec_t
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md)
  - [DecodeTimeOnly](DecodeTimeOnly.md)
  - [DecodeNumber](DecodeNumber.md)

## Notes and Other Information
- This is a static function internal to datetime.c, not part of the public API
- The function modifies the input string by inserting null terminators during parsing
- It handles both integer and fractional numeric inputs
- The function uses context from previously parsed fields (fmask) to make intelligent decisions about field interpretation
- Supports legacy 2-digit year formats and sets the is2digits flag accordingly
- Part of PostgreSQL's flexible datetime parsing system that can handle various input formats