# unicodeStyleFormat

## Location
[src/fe_utils/print.c:126-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L126-L138)

## Overview
A comprehensive structure that combines all Unicode/UTF-8 table formatting elements and defines the complete styling scheme for table output in PostgreSQL frontend utilities.

## Definition

```c
typedef struct unicodeStyleFormat
{
	unicodeStyleRowFormat row_style[2];
	unicodeStyleColumnFormat column_style[2];
	unicodeStyleBorderFormat border_style[2];
	const char *header_nl_left;
	const char *header_nl_right;
	const char *nl_left;
	const char *nl_right;
	const char *wrap_left;
	const char *wrap_right;
	bool		wrap_right_border;
} unicodeStyleFormat;
```
## Detailed Description
This structure serves as the master container for all Unicode table formatting styles in PostgreSQL's frontend utilities. It aggregates row, column, and border formatting structures along with additional formatting elements for headers, newlines, and text wrapping. The structure supports dual styling options (indicated by the array size of 2) for different formatting contexts and includes comprehensive control over table appearance in terminal output.

## Parameters / Member Variables
- `row_style[2]`: An array of two unicodeStyleRowFormat structures for different row formatting styles
- `column_style[2]`: An array of two unicodeStyleColumnFormat structures for different column formatting styles
- `border_style[2]`: An array of two unicodeStyleBorderFormat structures for different border formatting styles
- `*header_nl_left`: A pointer to the Unicode character string used for left side of header newlines
- `*header_nl_right`: A pointer to the Unicode character string used for right side of header newlines
- `*nl_left`: A pointer to the Unicode character string used for left side of regular newlines
- `*nl_right`: A pointer to the Unicode character string used for right side of regular newlines
- `*wrap_left`: A pointer to the Unicode character string used for left side of wrapped text lines
- `*wrap_right`: A pointer to the Unicode character string used for right side of wrapped text lines
- `wrap_right_border`: A boolean flag indicating whether to include right border when wrapping text
## Dependencies
- Functions called/Symbols referenced:
  - [unicodeStyleRowFormat](unicodeStyleRowFormat.md) (at Line 128)
  - [unicodeStyleColumnFormat](unicodeStyleColumnFormat.md) (at Line 129) 
  - [unicodeStyleBorderFormat](unicodeStyleBorderFormat.md) (at Line 130)
- Called from (representative examples):
  - (No direct references found - likely used through variable instantiation)

## Notes and Other Information
- This is the top-level structure that unifies all Unicode table formatting capabilities
- The dual-element arrays suggest support for different formatting modes or line weights
- Provides comprehensive control over table appearance including headers, borders, and text wrapping
- Part of PostgreSQL's sophisticated table formatting system for frontend utilities like psql
- Located in src/fe_utils/print.c as the central formatting structure
- Essential for creating properly formatted Unicode tables with full styling control