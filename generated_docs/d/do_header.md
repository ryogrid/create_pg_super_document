# do_header

## Location
[src/interfaces/libpq/fe-print.c:445-530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-print.c#L445-L530)

## Overview
Generates and formats the header row for PostgreSQL query result output, including column names and optional border decorations based on the specified print options.

## Definition

```c
static char *
do_header(FILE *fout, const PQprintOpt *po, const int nFields, int *fieldMax,
		  const char **fieldNames, unsigned char *fieldNotNum,
		  const int fs_len, const PGresult *res)
```
## Detailed Description
The  function creates formatted column headers for PostgreSQL query results in libpq. It handles multiple output formats and constructs appropriate headers with proper alignment and decorative borders. The function performs several key operations:

1. **Border Construction**: Creates decorative border strings using dashes and plus signs for standard format output
2. **Memory Management**: Allocates memory for border strings based on calculated total width requirements
3. **Format Detection**: Handles HTML3, standard (with borders), and plain text output formats
4. **Column Alignment**: Applies left or right alignment based on numeric content detection from fieldNotNum array
5. **Width Calculation**: Updates fieldMax array to accommodate header text that might be wider than data

The function constructs borders by calculating total width needed, including field separators and padding, then dynamically allocates and builds the border string character by character.

## Parameters / Member Variables
- : Output file stream for writing the formatted header
- : Print options structure containing formatting preferences (html3, standard, fieldSep)
- : Total number of fields (columns) in the result set
- : Array tracking maximum field width for each column (updated by this function)
- : Array of column names for the result set
- : Array indicating which fields contain non-numeric data for alignment
- : Length of the field separator string
- : PostgreSQL result set for accessing column metadata

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - libpq_gettext
  - PQfname
- Called from (representative examples):
  - winsize (src/interfaces/libpq/fe-print.c:287)

## Notes and Other Information
- Returns allocated border string on success, NULL on memory allocation failure
- The returned border string must be freed by the caller
- Updates the fieldMax array in-place to ensure headers fit properly
- HTML3 format uses table header tags with alignment attributes
- Standard format includes decorative borders above and below headers
- Field separator characters are converted to '+' characters in border strings
- Handles both left-aligned (text) and right-aligned (numeric) column headers