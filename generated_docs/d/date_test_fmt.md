# date_test_fmt

## Location
[src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.c:77-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.c#L77-L92)

## Overview
Tests date formatting functionality by converting a date structure to a formatted string representation using a specified format template.

## Definition
```c
static void date_test_fmt(date d, const char *fmt)
```

## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) test suite for Informix compatibility date formatting features. It validates the conversion of date structures to formatted string representations. The function uses `rfmtdate()` to format the input date structure according to the provided format specification, producing a formatted string output.

The function maintains a static counter for successful conversions and provides detailed output including return codes and formatted results. Unlike the parsing functions, this focuses on the output formatting aspect of date handling.

## Parameters / Member Variables
- `d`: A date structure containing the date to be formatted
- `fmt`: Format specification string that defines how the date should be formatted in the output

## Dependencies
- Functions called/Symbols referenced:
  - [rfmtdate](../r/rfmtdate.md) (formats date structure to string according to format)
  - [check_return](../c/check_return.md) (handles error reporting)
  - date (date structure type)
- Called from (representative examples):
  - [main](../m/main.md) (called multiple times in the test program with various format specifications)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Part of the ECPG test suite for Informix compatibility date formatting functions
- Uses static variable `i` to maintain a counter across function calls
- Complements the parsing functions by testing the output formatting aspect
- Uses a 200-byte buffer for formatted output
- Handles both successful formatting and error conditions
- Located in the expected output file for regression testing