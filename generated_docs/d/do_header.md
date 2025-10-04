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
- `*fout`: Output file stream for writing the formatted header
- `*po`: Print options structure containing formatting preferences (html3, standard, fieldSep)
- `nFields`: Total number of fields (columns) in the result set
- `*fieldMax`: Array tracking maximum field width for each column (updated by this function)
- `**fieldNames`: Array of column names for the result set
- `*fieldNotNum`: Array indicating which fields contain non-numeric data for alignment
- `fs_len`: Length of the field separator string
- `*res`: PostgreSQL result set for accessing column metadata
## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [libpq_gettext](../l/libpq_gettext.md)
  - [PQfname](../P/PQfname.md)
- Called from (representative examples):
  - [winsize](../w/winsize.md) (src/interfaces/libpq/fe-print.c:287)

## Notes and Other Information
- Returns allocated border string on success, NULL on memory allocation failure
- The returned border string must be freed by the caller
- Updates the fieldMax array in-place to ensure headers fit properly
- HTML3 format uses table header tags with alignment attributes
- Standard format includes decorative borders above and below headers
- Field separator characters are converted to '+' characters in border strings
- Handles both left-aligned (text) and right-aligned (numeric) column headers

## Simplified Source

```c
static char *do_header(FILE *fout, const PQprintOpt *po, const int nFields, int *fieldMax,
                       const char **fieldNames, unsigned char *fieldNotNum,
                       const int fs_len, const PGresult *res) {
    char *border = NULL;

    // Handle HTML3 format
    if (po->html3) {
        fputs("<tr>", fout);
    } else {
        // Calculate total width and allocate border string
        int tot = 0;
        for (int n = 0; n < nFields; n++)
            tot += fieldMax[n] + fs_len + (po->standard ? 2 : 0);
        if (po->standard) tot += fs_len * 2 + 2;

        border = malloc(tot + 1);
        if (!border) return NULL;

        // Build border string with dashes and plus signs
        char *p = border;
        if (po->standard) {
            for (char *fs = po->fieldSep; *fs++; *p++ = '+');
        }
        for (int j = 0; j < nFields; j++) {
            for (int len = fieldMax[j] + (po->standard ? 2 : 0); len--; *p++ = '-');
            if (po->standard || (j + 1) < nFields) {
                for (char *fs = po->fieldSep; *fs++; *p++ = '+');
            }
        }
        *p = '\0';

        if (po->standard) fprintf(fout, "%s\n", border);
    }

    // Print field separator for standard format
    if (po->standard) fputs(po->fieldSep, fout);

    // Print column headers
    for (int j = 0; j < nFields; j++) {
        const char *s = PQfname(res, j);

        if (po->html3) {
            fprintf(fout, "<th align=\"%s\">%s</th>",
                    fieldNotNum[j] ? "left" : "right", fieldNames[j]);
        } else {
            // Update field width if header is wider than data
            int n = strlen(s);
            if (n > fieldMax[j]) fieldMax[j] = n;

            // Print aligned header
            if (po->standard) {
                fprintf(fout, fieldNotNum[j] ? " %-*s " : " %*s ", fieldMax[j], s);
            } else {
                fprintf(fout, fieldNotNum[j] ? "%-*s" : "%*s", fieldMax[j], s);
            }

            if (po->standard || (j + 1) < nFields)
                fputs(po->fieldSep, fout);
        }
    }

    // Close header row
    if (po->html3) {
        fputs("</tr>\n", fout);
    } else {
        fprintf(fout, "\n%s\n", border);
    }

    return border;
}
```