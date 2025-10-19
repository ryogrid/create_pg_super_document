# refresh_utf8format

## Location
[src/fe_utils/print.c:3691-3745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3691-L3745)

## Overview
Configures the global UTF-8 unicode table formatting structure based on provided print options, setting up border characters and line styles for Unicode-based table output.

## Definition

```c
void
refresh_utf8format(const printTableOpt *opt)
```
## Detailed Description
This function refreshes the global  printTextFormat structure by configuring Unicode-based table formatting options. It sets up various rule types (TOP, MIDDLE, BOTTOM, DATA) with appropriate Unicode characters for borders, headers, and columns based on the provided line style options. The function maps Unicode style configurations to the internal print format structure, enabling consistent Unicode table rendering across PostgreSQL's frontend utilities.

The function operates on the global  variable and configures it according to the Unicode border, header, and column line styles specified in the input options. It handles four different rule types for table formatting: top border, middle separator, bottom border, and data rows.

## Parameters / Member Variables
- `*opt`: Pointer to printTableOpt structure containing Unicode formatting preferences including border_linestyle, header_linestyle, and column_linestyle settings
## Dependencies
- Functions called/Symbols referenced:
  - [printTableOpt](../p/printTableOpt.md) (parameter type)
  - [printTextFormat](../p/printTextFormat.md) (target structure type)
  - [unicodeStyleBorderFormat](../u/unicodeStyleBorderFormat.md) (border style structure)
  - [unicodeStyleRowFormat](../u/unicodeStyleRowFormat.md) (row style structure)  
  - [unicodeStyleColumnFormat](../u/unicodeStyleColumnFormat.md) (column style structure)
  - PRINT_RULE_TOP, PRINT_RULE_MIDDLE, PRINT_RULE_BOTTOM, PRINT_RULE_DATA (rule type constants)
- Called from (representative examples):
  - fmt (src/bin/psql/command.c:4627, 4642, 4657)
  - [main](../m/main.md) (src/bin/psql/startup.c:181)

## Notes and Other Information
- Modifies the global  variable which is used throughout PostgreSQL's table printing system
- The function sets the format name to "unicode" to identify this as Unicode-based formatting
- All Unicode styles currently share the same newline and wrap formatting characters
- This function is part of PostgreSQL's frontend utilities print system, primarily used by psql for table output formatting
- The function maps three different line style dimensions (border, header, column) into a unified table format structure

## Simplified Source

```c
void refresh_utf8format(const printTableOpt *opt) {
    printTextFormat *popt = &pg_utf8format;

    // Get style components based on options
    const unicodeStyleBorderFormat *border = &unicode_style.border_style[opt->unicode_border_linestyle];
    const unicodeStyleRowFormat *header = &unicode_style.row_style[opt->unicode_header_linestyle];
    const unicodeStyleColumnFormat *column = &unicode_style.column_style[opt->unicode_column_linestyle];

    popt->name = "unicode";

    // Configure top border line (table start)
    popt->lrule[PRINT_RULE_TOP].hrule = border->horizontal;
    popt->lrule[PRINT_RULE_TOP].leftvrule = border->down_and_right;
    popt->lrule[PRINT_RULE_TOP].midvrule = column->down_and_horizontal[opt->unicode_border_linestyle];
    popt->lrule[PRINT_RULE_TOP].rightvrule = border->down_and_left;

    // Configure middle separator line (header/data separator)
    popt->lrule[PRINT_RULE_MIDDLE].hrule = header->horizontal;
    popt->lrule[PRINT_RULE_MIDDLE].leftvrule = header->vertical_and_right[opt->unicode_border_linestyle];
    popt->lrule[PRINT_RULE_MIDDLE].midvrule = column->vertical_and_horizontal[opt->unicode_header_linestyle];
    popt->lrule[PRINT_RULE_MIDDLE].rightvrule = header->vertical_and_left[opt->unicode_border_linestyle];

    // Configure bottom border line (table end)
    popt->lrule[PRINT_RULE_BOTTOM].hrule = border->horizontal;
    popt->lrule[PRINT_RULE_BOTTOM].leftvrule = border->up_and_right;
    popt->lrule[PRINT_RULE_BOTTOM].midvrule = column->up_and_horizontal[opt->unicode_border_linestyle];
    popt->lrule[PRINT_RULE_BOTTOM].rightvrule = border->left_and_right;

    // Configure data row formatting (vertical separators only)
    popt->lrule[PRINT_RULE_DATA].hrule = "";
    popt->lrule[PRINT_RULE_DATA].leftvrule = border->vertical;
    popt->lrule[PRINT_RULE_DATA].midvrule = column->vertical;
    popt->lrule[PRINT_RULE_DATA].rightvrule = border->vertical;

    // Configure line wrap and newline formatting
    popt->midvrule_nl = column->vertical;
    popt->midvrule_wrap = column->vertical;
    popt->midvrule_blank = column->vertical;

    // Set common Unicode formatting elements
    popt->header_nl_left = unicode_style.header_nl_left;
    popt->header_nl_right = unicode_style.header_nl_right;
    popt->nl_left = unicode_style.nl_left;
    popt->nl_right = unicode_style.nl_right;
    popt->wrap_left = unicode_style.wrap_left;
    popt->wrap_right = unicode_style.wrap_right;
    popt->wrap_right_border = unicode_style.wrap_right_border;
}
```