# unicode_linestyle

## Location
[src/include/fe_utils/print.h:103-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/print.h#L103-L104)

## Overview
unicode_linestyle is an enumeration type that defines different Unicode line drawing styles used for table borders and separators in PostgreSQL's frontend printing utilities.

## Definition

(Defined in src/include/fe_utils/print.h:99-103)

## Detailed Description
The unicode_linestyle enumeration specifies different styles of Unicode line drawing characters that can be used when rendering table borders, column separators, and header lines. This enumeration allows users to choose between single-line and double-line Unicode characters for enhanced visual formatting of table output. The different line styles provide aesthetic variety and can help improve readability by creating distinct visual separation between different parts of table output.

## Parameters / Member Variables
- : Uses single-line Unicode drawing characters (value 0, default)
- : Uses double-line Unicode drawing characters for enhanced visual emphasis

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration type definition)
- Called from (representative examples):
  - [set_unicode_line_style](../s/set_unicode_line_style.md) (src/bin/psql/command.c:4491)
  - [printTableOpt](../p/printTableOpt.md) (src/include/fe_utils/print.h:141-143) - used for unicode_border_linestyle, unicode_column_linestyle, and unicode_header_linestyle

## Notes and Other Information
- This enumeration is used in three different contexts within printTableOpt structure: border lines, column separators, and header lines, allowing independent control of each element's line style
- Essential for providing Unicode-aware table formatting in PostgreSQL's command-line tools
- The enumeration supports terminals and applications that can display Unicode box-drawing characters
- Single-line style uses characters like ─ │ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼
- Double-line style uses characters like ═ ║ ╔ ╗ ╚ ╝ ╠ ╣ ╦ ╩ ╬
- Enhances visual appeal and readability of table output in Unicode-capable terminals