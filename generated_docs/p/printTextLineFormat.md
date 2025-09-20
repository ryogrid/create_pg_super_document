# printTextLineFormat

## Location
[src/include/fe_utils/print.h:43-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/print.h#L43-L50)

## Overview
A structure that defines the line drawing characters used for formatting text tables in PostgreSQL frontend utilities.

## Definition

```c
typedef struct printTextLineFormat
{
	/* Line drawing characters to be used in various contexts */
	const char *hrule;			/* horizontal line character */
	const char *leftvrule;		/* left vertical line (+horizontal) */
	const char *midvrule;		/* intra-column vertical line (+horizontal) */
	const char *rightvrule;		/* right vertical line (+horizontal) */
} printTextLineFormat;
```
## Detailed Description
The printTextLineFormat structure encapsulates the character set used for drawing table borders and separators in text-based table output. This structure allows for customization of table appearance by defining different character sets for horizontal rules and various types of vertical rule intersections. It's particularly useful for creating ASCII art-style tables with consistent formatting across different parts of the table structure.

## Parameters / Member Variables
- : Horizontal line character used for drawing horizontal borders and separators
- : Character used at the left edge where vertical and horizontal lines intersect
- : Character used at internal column boundaries where vertical and horizontal lines intersect
- : Character used at the right edge where vertical and horizontal lines intersect

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - [_print_horizontal_line](_print_horizontal_line.md) (src/fe_utils/print.c:598)
  - [print_aligned_text](print_aligned_text.md) (src/fe_utils/print.c:641)
  - [print_aligned_vertical_line](print_aligned_vertical_line.md) (src/fe_utils/print.c:1233)
  - [print_aligned_vertical](print_aligned_vertical.md) (src/fe_utils/print.c:1330)
  - [printTextFormat](printTextFormat.md) (src/include/fe_utils/print.h:85)

## Notes and Other Information
This structure is part of the frontend utilities printing system and is used to maintain consistency in table formatting across different PostgreSQL client tools like psql. The character choices can vary depending on the desired visual style (e.g., ASCII vs Unicode box-drawing characters).