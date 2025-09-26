# PQprintTuples

## Location
[src/interfaces/libpq/fe-print.c:671-754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-print.c#L671-L754)

## Overview
A public libpq function that prints PostgreSQL query result tuples to a file stream in a simple tabular format with optional column headers and borders.

## Definition

```c
void
PQprintTuples(const PGresult *res,
			  FILE *fout,		/* output stream */
			  int PrintAttNames,	/* print attribute names or not */
			  int TerseOutput,	/* delimiter bars or not? */
			  int colWidth		/* width of column, if 0, use variable width */
)
```
## Detailed Description
 is a legacy public function in libpq that provides a simple way to print PostgreSQL query results in a tabular format. It offers basic formatting options and serves as a simpler alternative to the more advanced  function. The function creates a basic table layout with the following features:

1. **Column Headers**: Optionally prints field names as column headers when PrintAttNames is enabled
2. **Border Control**: Can output with or without decorative borders (pipe characters and dashes)
3. **Fixed/Variable Width**: Supports both fixed-width columns and variable-width formatting
4. **Memory Management**: Dynamically allocates border strings and properly handles cleanup

The function uses a format string approach to control output formatting and calculates border widths based on a fixed formula (nFields * 14 characters). For terse output, it omits border characters and separators, providing clean space-separated output.

## Parameters / Member Variables
- : PostgreSQL result set containing the data to be printed
- : Output file stream where the formatted table will be written
- : Boolean flag indicating whether to print column names as headers (1 = yes, 0 = no)
- : Boolean flag controlling border output (1 = no borders, 0 = include borders)
- : Fixed column width in characters; if 0, uses variable width formatting

## Dependencies
- Functions called/Symbols referenced:
  - PQnfields
  - PQntuples  
  - malloc
  - libpq_gettext
  - PQfname
  - PQgetvalue
- Called from (representative examples):
  - PQnoPasswordSupplied (referenced in src/interfaces/libpq/libpq-fe.h:661)

## Notes and Other Information
- This is a public libpq API function available to client applications
- Uses a fixed border width calculation of nFields * 14 characters
- Returns early if memory allocation fails, printing an error message
- Handles null field values by substituting empty strings
- Provides basic table formatting without advanced features like alignment detection
- Format string is dynamically constructed based on colWidth parameter
- Memory for borders is properly freed before function exit
- Skips output entirely if the result set has no fields