# printTableOpt

## Location
[src/include/fe_utils/print.h:111-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/print.h#L111-L144)

## Overview
A comprehensive configuration structure that controls all aspects of table formatting and output in PostgreSQL frontend utilities, including layout, styling, pagination, and field separators.

## Definition

```c
typedef struct printTableOpt
{
	enum printFormat format;	/* see enum above */
	unsigned short int expanded;	/* expanded/vertical output (if supported
									 * by output format); 0=no, 1=yes, 2=auto */
	printXheaderWidthType expanded_header_width_type;	/* width type for header
														 * line in expanded mode */
	int			expanded_header_exact_width;	/* explicit width for header
												 * line in expanded mode */
	unsigned short int border;	/* Print a border around the table. 0=none,
								 * 1=dividing lines, 2=full */
	unsigned short int pager;	/* use pager for output (if to stdout and
								 * stdout is a tty) 0=off 1=on 2=always */
	int			pager_min_lines;	/* don't use pager unless there are at
									 * least this many lines */
	bool		tuples_only;	/* don't output headers, row counts, etc. */
	bool		start_table;	/* print start decoration, eg <table> */
	bool		stop_table;		/* print stop decoration, eg </table> */
	bool		default_footer; /* allow "(xx rows)" default footer */
	unsigned long prior_records;	/* start offset for record counters */
	const printTextFormat *line_style;	/* line style (NULL for default) */
	struct separator fieldSep;	/* field separator for unaligned text mode */
	struct separator recordSep; /* record separator for unaligned text mode */
	char		csvFieldSep[2]; /* field separator for csv format */
	bool		numericLocale;	/* locale-aware numeric units separator and
								 * decimal marker */
	char	   *tableAttr;		/* attributes for HTML <table ...> */
	int			encoding;		/* character encoding */
	int			env_columns;	/* $COLUMNS on psql start, 0 is unset */
	int			columns;		/* target width for wrapped format */
	unicode_linestyle unicode_border_linestyle;
	unicode_linestyle unicode_column_linestyle;
	unicode_linestyle unicode_header_linestyle;
} printTableOpt;
```
## Detailed Description
The printTableOpt structure serves as the central configuration hub for all table output formatting in PostgreSQL frontend utilities. It encompasses every aspect of table presentation from basic layout options (borders, pagination) to advanced formatting features (Unicode styling, locale-aware numbering, CSV output). This structure allows fine-grained control over table appearance and behavior, supporting multiple output formats including text, HTML, CSV, and others.

## Parameters / Member Variables
- : Output format type (text, HTML, CSV, etc.) as defined by printFormat enum
- : Controls expanded/vertical output mode (0=no, 1=yes, 2=auto)
- : Width calculation method for headers in expanded mode
- : Explicit width setting for headers in expanded mode
- : Border display level (0=none, 1=dividing lines, 2=full borders)
- : Pager usage control (0=off, 1=on, 2=always)
- : Minimum line count threshold for pager activation
- : When true, suppresses headers, row counts, and other metadata
- : Controls printing of table start decorations (e.g., HTML )
- : Controls printing of table end decorations (e.g., HTML )
- : Enables/disables default footer showing row count
- : Starting offset for record counter display
- : Pointer to printTextFormat structure defining line drawing style
- : Field separator configuration for unaligned text mode
- : Record separator configuration for unaligned text mode
- : Field separator character array for CSV format output
- : Enables locale-aware formatting for numeric values
- : HTML table attributes string for HTML format output
- : Character encoding specification
- : Initial  environment variable value
- : Target width for wrapped format output
- : Unicode style for table borders
- : Unicode style for column separators
- : Unicode style for header separators

## Dependencies
- Functions called/Symbols referenced:
  - printFormat (enum)
  - [printXheaderWidthType](printXheaderWidthType.md) (type)
  - [printTextFormat](printTextFormat.md) (structure)
  - [separator](../s/separator.md) (structure)
  - [unicode_linestyle](../u/unicode_linestyle.md) (type)
- Called from (representative examples):
  - [describeOneTableDetails](../d/describeOneTableDetails.md) (src/bin/psql/describe.c:1536)
  - [describeRoles](../d/describeRoles.md) (src/bin/psql/describe.c:3619)
  - [describePublications](../d/describePublications.md) (src/bin/psql/describe.c:6420)
  - [print_aligned_vertical_line](print_aligned_vertical_line.md) (src/fe_utils/print.c:1225)
  - [PageOutput](../P/PageOutput.md) (src/fe_utils/print.c:3089)
  - [printTableInit](printTableInit.md) (src/fe_utils/print.c:3172)
  - [get_line_style](../g/get_line_style.md) (src/fe_utils/print.c:3677)
  - refresh_utf8format (src/fe_utils/print.c:3691)
  - [printTableContent](printTableContent.md) (src/include/fe_utils/print.h:165)
  - [printQueryOpt](printQueryOpt.md) (src/include/fe_utils/print.h:185)

## Notes and Other Information
This structure is essential to PostgreSQL's table output system and is used extensively in psql and other frontend utilities. It provides a unified interface for controlling table appearance across different output formats and environments. The Unicode linestyle fields enable sophisticated table styling with Unicode box-drawing characters, while the separator structures allow flexible field and record delimiting for various text formats.