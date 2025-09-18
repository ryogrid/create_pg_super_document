# csv_escaped_print

## Location
[src/fe_utils/print.c:1840-1854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L1840-L1854)

## Overview
Prints a string to a file stream with proper CSV escaping, wrapping the string in double quotes and escaping any internal double quotes by doubling them.

## Definition
```c
static void csv_escaped_print(const char *str, FILE *fout)
```

## Detailed Description
This function implements RFC 4180 CSV standard escaping for string values. It wraps the input string in double quotes and escapes any double quote characters within the string by doubling them (converting " to ""). This ensures that the output can be safely parsed by CSV readers without ambiguity about field boundaries or embedded quote characters.

The function processes the input string character by character, writing each character to the output stream while checking for double quote characters that need special handling. This simple but essential utility function ensures data integrity when outputting PostgreSQL query results in CSV format.

## Parameters / Member Variables
- `str`: Input string to be escaped and printed in CSV format
- `fout`: File stream where the escaped string will be written

## Dependencies
- Functions called/Symbols referenced:
  - fputc (standard C library function)
- Called from (representative examples):
  - [csv_print_field](csv_print_field.md)

## Notes and Other Information
This is a utility function used internally by PostgreSQL's CSV output formatting system. The function assumes the input string is null-terminated and handles empty strings correctly by outputting just the wrapping double quotes. The escaping follows the standard CSV convention where double quotes within field values are escaped by doubling them, making the output compatible with standard CSV parsers and spreadsheet applications.