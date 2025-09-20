# EncodeSpecialTimestamp

## Location
[src/interfaces/ecpg/pgtypeslib/timestamp.c:195-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/timestamp.c#L195-L205)

## Overview
Converts reserved timestamp data type values (infinity and negative infinity) to their string representations.

## Definition

```c
struct tm	tt,
			   *tm = &tt;
```
## Detailed Description
EncodeSpecialTimestamp is a utility function that handles the encoding of special timestamp values that represent infinity concepts in PostgreSQL. The function specifically deals with two special timestamp values: TIMESTAMP_IS_NOBEGIN (negative infinity) and TIMESTAMP_IS_NOEND (positive infinity). These special values are used internally by PostgreSQL to represent timestamps that are conceptually before all other timestamps or after all other timestamps, respectively.

The function performs a simple conversion by checking the special timestamp type and copying the appropriate string constant to the output buffer. If an invalid timestamp value is passed, the function raises an ERROR using elog().

## Parameters / Member Variables
- `dt`: The timestamp value to be encoded, expected to be one of the special timestamp values
- `str`: Output buffer where the string representation will be written

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_IS_NOBEGIN (macro to check for negative infinity)
  - TIMESTAMP_IS_NOEND (macro to check for positive infinity)
  - strcpy (standard C library function)
  - elog (PostgreSQL logging/error function)
  - EARLY (string constant for negative infinity representation)
  - LATE (string constant for positive infinity representation)
- Called from (representative examples):
  - JsonEncodeDateTime (JSON encoding functions)
  - [timestamp_out](../t/timestamp_out.md) (timestamp output function)
  - [timestamptz_out](../t/timestamptz_out.md) (timestamptz output function)
  - [timestamptz_to_str](../t/timestamptz_to_str.md) (timestamptz string conversion)
  - [PGTYPEStimestamp_to_asc](../P/PGTYPEStimestamp_to_asc.md) (ECPG timestamp conversion)

## Notes and Other Information
- This function assumes the input timestamp is indeed a special value and will generate an error for regular timestamp values
- The function does not perform bounds checking on the output buffer - the caller must ensure sufficient space
- The EARLY and LATE constants are defined elsewhere in the codebase and represent the string literals for infinity values
- This function is part of PostgreSQL's timestamp handling infrastructure and is used across multiple modules including JSON encoding and ECPG interfaces