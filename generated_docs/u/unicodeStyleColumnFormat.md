# unicodeStyleColumnFormat

## Location
[src/fe_utils/print.c:108-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L108-L114)

## Overview
A structure that defines the formatting characters for table column separators and intersections in Unicode/UTF-8 style output for PostgreSQL frontend utilities.

## Definition

```c
typedef struct unicodeStyleColumnFormat
{
	const char *vertical;
	const char *vertical_and_horizontal[2];
	const char *up_and_horizontal[2];
	const char *down_and_horizontal[2];
} unicodeStyleColumnFormat;
```
## Detailed Description
This structure contains the Unicode characters used for formatting table columns in PostgreSQL's frontend utilities. It provides characters for vertical lines and various intersection points where vertical and horizontal lines meet. The structure supports different types of column separators and junction points needed for proper table rendering in terminal output with Unicode/UTF-8 characters.

## Parameters / Member Variables
- `*vertical`: A pointer to the Unicode character string used for drawing vertical column separator lines
- `*vertical_and_horizontal[2]`: An array of two character string pointers for cross-shaped intersections where vertical and horizontal lines meet, supporting different line styles
- `*up_and_horizontal[2]`: An array of two character string pointers for T-shaped intersections where horizontal lines meet a vertical line from above
- `*down_and_horizontal[2]`: An array of two character string pointers for inverted T-shaped intersections where horizontal lines meet a vertical line from below
## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure definition)
- Called from (representative examples):
  - [unicodeStyleFormat](unicodeStyleFormat.md) (at src/fe_utils/print.c:129)
  - [refresh_utf8format](../r/refresh_utf8format.md) (at src/fe_utils/print.c:3697)

## Notes and Other Information
- This structure is part of PostgreSQL's table formatting system for frontend utilities like psql
- The dual-element arrays support different formatting contexts, possibly for different line weights or styles
- Works in conjunction with unicodeStyleRowFormat and other Unicode style structures
- Essential for creating properly formatted table intersections and column separators
- Located in src/fe_utils/print.c alongside other table formatting structures