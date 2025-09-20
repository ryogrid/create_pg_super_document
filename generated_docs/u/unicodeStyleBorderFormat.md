# unicodeStyleBorderFormat

## Location
[src/fe_utils/print.c:116-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L116-L124)

## Overview
A structure that defines the formatting characters for table border elements in Unicode/UTF-8 style output for PostgreSQL frontend utilities.

## Definition

```c
typedef struct unicodeStyleBorderFormat
{
	const char *up_and_right;
	const char *vertical;
	const char *down_and_right;
	const char *horizontal;
	const char *down_and_left;
	const char *left_and_right;
} unicodeStyleBorderFormat;
```
## Detailed Description
This structure contains the Unicode characters used for formatting the outer borders of tables in PostgreSQL's frontend utilities. It provides all the necessary corner pieces and edge elements needed to draw complete table borders, including corner connections and straight border lines. The structure is specifically designed to support the rendering of table perimeters with proper Unicode/UTF-8 border characters.

## Parameters / Member Variables
- : A pointer to the Unicode character string for bottom-left corner (connecting upward and rightward lines)
- : A pointer to the Unicode character string used for drawing vertical border lines
- : A pointer to the Unicode character string for top-left corner (connecting downward and rightward lines)
- : A pointer to the Unicode character string used for drawing horizontal border lines
- : A pointer to the Unicode character string for top-right corner (connecting downward and leftward lines)
- : A pointer to the Unicode character string for horizontal connectors or bottom border elements

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure definition)
- Called from (representative examples):
  - [unicodeStyleFormat](unicodeStyleFormat.md) (at src/fe_utils/print.c:130)
  - refresh_utf8format (at src/fe_utils/print.c:3695)

## Notes and Other Information
- This structure is part of PostgreSQL's table formatting system for frontend utilities like psql
- Provides all the corner and edge pieces necessary for complete table border rendering
- Works together with unicodeStyleRowFormat and unicodeStyleColumnFormat to create complete table layouts
- Essential for drawing the outer perimeter of formatted tables in terminal output
- Located in src/fe_utils/print.c as part of the Unicode table formatting infrastructure