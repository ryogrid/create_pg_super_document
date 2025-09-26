# PGTYPESdate_from_asc

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:47-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L47-L100)

## Overview
Parses a string representation of a date and converts it to a PostgreSQL date value, supporting various date formats and providing error handling for invalid input.

## Definition
```c
date PGTYPESdate_from_asc(char *str, char **endptr)
```

## Detailed Description
PGTYPESdate_from_asc is a comprehensive date parsing function that converts string representations of dates into PostgreSQL date values. The function supports multiple date formats and special values like epoch dates. It uses PostgreSQL's standard date parsing infrastructure (ParseDateTime and DecodeDateTime) to handle various input formats. The function validates input length, parses the date string into components, decodes the components into a standard date structure, and finally converts the result to PostgreSQL's internal date representation (days since 2000-01-01). Error handling is robust, setting errno and returning INT_MIN for invalid inputs.

## Parameters / Member Variables
- `str`: The string representation of the date to parse (must not exceed MAXDATELEN characters)
- `endptr`: Optional pointer to a char pointer that will be set to point to the character following the parsed date string. Can be NULL if not needed.

## Dependencies
- Functions called/Symbols referenced:
  - [ParseDateTime](ParseDateTime.md) (PostgreSQL date parsing function)
  - [DecodeDateTime](../D/DecodeDateTime.md) (PostgreSQL date decoding function)  
  - [GetEpochTime](../G/GetEpochTime.md) (epoch time handling function)
  - [date2j](../d/date2j.md) (Julian date conversion function)
  - MAXDATELEN, MAXDATEFIELDS (parsing buffer size constants)
  - DTK_DATE, DTK_EPOCH (date type constants)
  - PGTYPES_DATE_BAD_DATE (error constant)
- Called from (representative examples):
  - [ecpg_get_data](../e/ecpg_get_data.md) (ECPG data retrieval function)
  - [main](../m/main.md) (in test cases and array handling)

## Notes and Other Information
- Returns INT_MIN and sets errno to PGTYPES_DATE_BAD_DATE on parsing errors
- Supports European date format parsing (though currently disabled with EuroDates = false)
- Input string length is limited to MAXDATELEN characters for security
- The function handles special date values like 'epoch' 
- Uses PostgreSQL's Julian date system with 2000-01-01 as the reference point
- Part of the ECPG pgtypeslib interface for string-to-date conversions
- Located in src/interfaces/ecpg/pgtypeslib/datetime.c:47-100