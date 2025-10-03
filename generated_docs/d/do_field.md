# do_field

## Location
[src/interfaces/libpq/fe-print.c:330-444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-print.c#L330-L444)

## Overview
Processes and formats a single field value from a PostgreSQL query result for display, handling numeric detection, memory allocation, and output formatting based on print options.

## Definition

```c
static bool
do_field(const PQprintOpt *po, const PGresult *res,
		 const int i, const int j, const int fs_len,
		 char **fields,
		 const int nFields, char const **fieldNames,
		 unsigned char *fieldNotNum, int *fieldMax,
		 const int fieldMaxLen, FILE *fout)
```
## Detailed Description
The  function is a core component of PostgreSQL's result formatting system in libpq. It processes individual field values from query results and formats them for output according to specified print options. The function handles several key responsibilities:

1. **Value Extraction**: Retrieves field values using  and 
2. **Numeric Detection**: Analyzes field content to determine if it contains numeric data for proper alignment
3. **Memory Management**: Allocates memory for field storage when needed for alignment or HTML output
4. **Output Formatting**: Supports multiple output formats including plain text, aligned columns, expanded format, and HTML3
5. **Empty Field Handling**: Manages null or empty field values based on formatting options

The function uses sophisticated numeric detection logic that considers digits, decimal points, scientific notation (E/e), spaces, and negative signs while applying additional validation to prevent false positives.

## Parameters / Member Variables
- `*po`: Print options structure containing formatting preferences (align, expanded, html3, fieldSep)
- `*res`: PostgreSQL result set containing the data to be formatted
- `i`: Row index in the result set
- `j`: Column index in the result set
- `fs_len`: Length of the field separator string
- `**fields`: Array to store allocated field values for aligned output
- `nFields`: Total number of fields (columns) in the result set
- `**fieldNames`: Array of column names for the result set
- `*fieldNotNum`: Array tracking which fields contain non-numeric data
- `*fieldMax`: Array tracking maximum field width for each column
- `fieldMaxLen`: Maximum length among all field names
- `*fout`: Output file stream for writing formatted results
## Dependencies
- Functions called/Symbols referenced:
  - [PQgetlength](../P/PQgetlength.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - malloc
  - [libpq_gettext](../l/libpq_gettext.md)
- Called from (representative examples):
  - [winsize](../w/winsize.md) (src/interfaces/libpq/fe-print.c:257)

## Notes and Other Information
- Returns  on memory allocation failure,  on success
- Implements multi-byte character support through  for proper character boundary detection
- The numeric detection algorithm is designed to handle most common numeric formats but acknowledges it's not bulletproof
- Memory allocation only occurs for non-expanded output modes that require alignment or HTML formatting
- The function includes a goto label  for handling empty field output in non-HTML modes
- Supports both left and right alignment based on numeric content detection