# printTableAddHeader

## Location
src/fe_utils/print.c: 3220 - 3259

## Overview
Adds a column header to a previously initialized printTableContent structure with optional translation and alignment specification.

## Definition
```c
void printTableAddHeader(printTableContent *const content, char *header,
                        const bool translate, const char align)
```

## Detailed Description
This function adds a header string to the next available column position in a printTableContent structure. It validates that the column count has not been exceeded, performs multibyte string validation on the header text, and optionally translates the header through gettext if NLS (Native Language Support) is enabled and the translate parameter is true. The function also sets the alignment character for the column. Headers are stored as pointers to the original strings, so callers must ensure the header strings remain valid for the table's lifetime.

## Parameters / Member Variables
- `content`: Pointer to the printTableContent structure to add the header to
- `header`: Header string to add (not duplicated - caller must maintain)
- `translate`: Boolean flag indicating whether to translate the header through gettext
- `align`: Character specifying column alignment ('l' for left, 'r' for right)

## Dependencies
- Functions called/Symbols referenced:
  - printTableContent (content structure type)
  - EXIT_FAILURE (standard exit code for failure)
  - mbvalidate (multibyte string validation function)
- Called from (representative examples):
  - printCrosstab (src/bin/psql/crosstabview.c:304, 332)
  - describeOneTableDetails (src/bin/psql/describe.c:2054)
  - describeRoles (src/bin/psql/describe.c:3672, 3673, 3676)
  - describePublications (src/bin/psql/describe.c:6431-6439)
  - printQuery (src/fe_utils/print.c:3569)

## Notes and Other Information
- Must be called after printTableInit and before adding cell content
- Headers are not duplicated - caller must ensure header string availability
- Validates multibyte encoding using the encoding specified in content->opt->encoding
- Translation support depends on ENABLE_NLS compile-time flag
- Exits with failure if column count is exceeded
- Advances internal header and align pointers for next header addition
- Alignment character typically 'l' (left) or 'r' (right) but other values may be supported
- Used extensively in psql's describe commands and query result formatting