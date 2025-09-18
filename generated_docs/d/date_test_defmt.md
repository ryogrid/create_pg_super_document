# date_test_defmt

## Location
[src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.c:52-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.c#L52-L76)

## Overview
Tests formatted date parsing functionality by converting a string input using a specified format to a date structure and then back to a standardized string format.

## Definition
```c
static void date_test_defmt(const char *fmt, const char *input)
```

## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) test suite for Informix compatibility date formatting features. It validates the parsing of date strings using custom format specifications. The function uses `rdefmtdate()` to parse the input string according to the provided format into a date structure, then uses `rdatestr()` to convert it back to a standard string format for verification.

Like `date_test_strdate`, this function maintains a static counter and provides detailed output for both successful conversions and error conditions.

## Parameters / Member Variables
- `fmt`: Format specification string that defines how the input date string should be parsed
- `input`: The date string to be parsed according to the format specification

## Dependencies
- Functions called/Symbols referenced:
  - [rdefmtdate](../r/rdefmtdate.md) (parses formatted string to date structure)
  - [rdatestr](../r/rdatestr.md) (converts date structure to standard string)  
  - [check_return](../c/check_return.md) (handles error reporting)
  - date (date structure type)
- Called from (representative examples):
  - [main](../m/main.md) (called extensively in the test program with various format/input combinations)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Part of the ECPG test suite for Informix compatibility formatted date functions
- Uses static variable `i` to maintain a counter across function calls
- Extensively tested in main() with various date format patterns
- Handles both successful parsing and error conditions
- Located in the expected output file for regression testing