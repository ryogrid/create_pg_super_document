# ParseFractionalSecond

## Location
[src/backend/utils/adt/datetime.c:709-753](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L709-L753)

## Overview
Parses fractional seconds from a string and converts the result to integer microseconds for PostgreSQL's internal time representation.

## Definition
```c
static int ParseFractionalSecond(char *cp, fsec_t *fsec)
```

## Detailed Description
ParseFractionalSecond is a wrapper around ParseFraction that specifically handles fractional seconds in time/date parsing. It takes the fractional part of a second (as a decimal string starting with '.') and converts it to PostgreSQL's internal fsec_t representation, which stores microseconds as an integer. The function uses rint() to perform proper rounding when converting from the floating-point fraction to integer microseconds.

This function is essential for handling sub-second precision in PostgreSQL's time types, ensuring accurate conversion from string representations to the database's internal microsecond-based storage format.

## Parameters / Member Variables
- `cp`: Pointer to character string starting with decimal point, representing the fractional seconds to parse
- `fsec`: Output parameter that receives the parsed fractional seconds as integer microseconds (fsec_t type)

## Dependencies
- Functions called/Symbols referenced:
  - ParseFraction (to parse the fractional part as double)
  - fsec_t (PostgreSQL's fractional seconds type)
  - rint (rounding function)
- Called from (representative examples):
  - DecodeTimeCommon
  - DecodeNumber

## Notes and Other Information
- Returns 0 on success, or error code from ParseFraction on failure
- Converts fractional seconds to microseconds by multiplying by 1,000,000 and rounding
- fsec_t is PostgreSQL's internal type for storing fractional seconds as integer microseconds
- Uses proper rounding (rint) to handle floating-point precision issues when converting to integer microseconds