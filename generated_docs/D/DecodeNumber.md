# DecodeNumber

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1197-1305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1197-L1305)

## Overview
DecodeNumber interprets a plain numeric field as a date value within the context of previously parsed fields, implementing PostgreSQL's intelligent date parsing logic.

## Definition

```c
static int
DecodeNumber(int flen, char *str, int fmask,
			 int *tmask, struct tm *tm, fsec_t *fsec, bool *is2digits, bool EuroDates)
```
## Detailed Description
This function is a central component of PostgreSQL's date/time parsing system that interprets numeric fields based on contextual information. It implements sophisticated logic to determine whether a numeric field represents a year, month, day, or time component based on:

1. **Field length and format**: Handles various numeric formats including decimals and concatenated fields
2. **Previously parsed fields**: Uses the fmask to understand what has already been parsed
3. **Date ordering preferences**: Respects the DateOrder setting (YMD, DMY, MDY)
4. **Text month context**: Behaves differently when a textual month name has been encountered

The function handles special cases like:
- Day-of-year format (3-digit numbers when only year is known)
- Fractional seconds parsing
- 2-digit year detection and marking
- Concatenated date/time fields
- Multiple date format ambiguities (DD-MON-YYYY, MON-DD-YYYY, YYYY-MON-DD)

## Parameters / Member Variables
- : Length of the numeric field being processed
- : String containing the numeric field to decode
- : Boolean indicating if a textual month name was previously encountered
- : Bitmask indicating which date/time fields have already been parsed
- : Pointer to bitmask that will be updated with the newly identified field type
- : Pointer to pg_tm structure where the parsed value will be stored
- : Pointer to fractional seconds storage (used for decimal values)
- : Pointer to boolean flag indicating if a 2-digit year was processed

## Dependencies
- Functions called/Symbols referenced:
  - [strtoint](../s/strtoint.md)
  - [DecodeNumberField](DecodeNumberField.md)
  - [ParseFractionalSecond](../P/ParseFractionalSecond.md)
  - DTK_M, DTK_DATE_M (field mask macros)
  - YEAR, MONTH, DAY, DOY (field type constants)
  - DATEORDER_YMD, DATEORDER_DMY, DATEORDER_MDY
  - DTERR_FIELD_OVERFLOW, DTERR_BAD_FORMAT (error constants)
  - struct pg_tm
  - fsec_t
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md)
  - [DecodeTimeOnly](DecodeTimeOnly.md)
  - [DecodeDate](DecodeDate.md)

## Notes and Other Information
- This is a static function internal to datetime.c, not part of the public API
- Implements PostgreSQL's paranoid approach to date parsing to avoid ambiguities
- The function uses a state machine approach based on previously parsed fields
- Handles legacy 2-digit year formats with proper flagging for later adjustment
- Supports day-of-year parsing (e.g., 2023.365 for the 365th day of 2023)
- The logic prioritizes unambiguous interpretations and follows configured date ordering preferences
- Critical for supporting PostgreSQL's flexible date input format compatibility