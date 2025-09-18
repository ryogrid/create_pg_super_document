# date_test_strdate

## Location
[src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.c:27-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.c#L27-L51)

## Overview
Tests the string-to-date parsing functionality by converting a string input to a date structure and then back to a string format for validation.

## Definition


## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) test suite for Informix compatibility features. It validates the round-trip conversion of date strings: string → date structure → string. The function uses the  function to parse the input string into a date structure, then uses  to convert it back to a string format, printing the results or error information.

The function maintains a static counter to number the successful conversions and uses printf statements to display the conversion results and status codes.

## Parameters / Member Variables
- : A string representation of a date to be parsed and validated

## Dependencies
- Functions called/Symbols referenced:
  - [rstrdate](../r/rstrdate.md) (parses string to date structure)
  - [rdatestr](../r/rdatestr.md) (converts date structure to string)
  - [check_return](../c/check_return.md) (handles error reporting)
  - date (date structure type)
- Called from (representative examples):
  - [main](../m/main.md) (in the test program)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- Part of the ECPG test suite for Informix compatibility date/time functions
- Uses static variable  to maintain a counter across function calls
- Handles both successful conversions and error cases
- Located in the expected output file for regression testing