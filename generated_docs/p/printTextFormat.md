# printTextFormat

## Location
[src/include/fe_utils/print.h:81-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/print.h#L81-L97)

## Overview
A comprehensive structure that defines a complete text table formatting style, including line drawing characters, continuation markers, and wrapping indicators for PostgreSQL frontend table output.

## Definition

```c
typedef struct printTextFormat
{
	/* A complete line style */
	const char *name;			/* for display purposes */
	printTextLineFormat lrule[4];	/* indexed by enum printTextRule */
	const char *midvrule_nl;	/* vertical line for continue after newline */
	const char *midvrule_wrap;	/* vertical line for wrapped data */
	const char *midvrule_blank; /* vertical line for blank data */
	const char *header_nl_left; /* left mark after newline */
	const char *header_nl_right;	/* right mark for newline */
	const char *nl_left;		/* left mark after newline */
	const char *nl_right;		/* right mark for newline */
	const char *wrap_left;		/* left mark after wrapped data */
	const char *wrap_right;		/* right mark for wrapped data */
	bool		wrap_right_border;	/* use right-hand border for wrap marks
									 * when border=0? */
} printTextFormat;
```
## Detailed Description
The printTextFormat structure represents a complete formatting theme for text-based table output in PostgreSQL frontend utilities. It encompasses not only basic line drawing characters but also sophisticated handling of text wrapping, line continuation, and different table sections (headers vs data). The structure allows for fine-grained control over table appearance, supporting various visual styles from simple ASCII to elaborate Unicode box-drawing formats.

## Parameters / Member Variables
- `*name`: Display name for this formatting style, used for identification purposes
- `lrule[4]`: Array of printTextLineFormat structures indexed by printTextRule enum values for different table regions
- `*midvrule_nl`: Vertical line character used when continuing content after a newline
- `*midvrule_wrap`: Vertical line character used for wrapped data lines
- `*midvrule_blank`: Vertical line character used for blank data cells
- `*header_nl_left`: Left-side marker character used after newlines in header sections
- `*header_nl_right`: Right-side marker character used for newlines in header sections
- `*nl_left`: Left-side marker character used after newlines in data sections
- `*nl_right`: Right-side marker character used for newlines in data sections
- `*wrap_left`: Left-side marker character used after wrapped data
- `*wrap_right`: Right-side marker character used for wrapped data
- `wrap_right_border`: Boolean flag controlling whether to use right-hand border for wrap marks when border=0
## Dependencies
- Functions called/Symbols referenced:
  - [printTextLineFormat](printTextLineFormat.md) (embedded structure)
- Called from (representative examples):
  - [_print_horizontal_line](_print_horizontal_line.md) (src/fe_utils/print.c:595)
  - [print_aligned_text](print_aligned_text.md) (src/fe_utils/print.c:640)
  - [print_aligned_vertical](print_aligned_vertical.md) (src/fe_utils/print.c:1329)
  - [setDecimalLocale](../s/setDecimalLocale.md) (src/fe_utils/print.c:3676)
  - [refresh_utf8format](../r/refresh_utf8format.md) (src/fe_utils/print.c:3693)
  - [printTableOpt](printTableOpt.md) (src/include/fe_utils/print.h:131)

## Notes and Other Information
This structure is central to PostgreSQL's table formatting system and enables the creation of visually appealing, readable table output. The array of printTextLineFormat structures allows different formatting for different parts of the table (top, middle, bottom, data). The wrap and newline handling features make it suitable for complex data display scenarios where content may exceed column widths or contain embedded newlines.