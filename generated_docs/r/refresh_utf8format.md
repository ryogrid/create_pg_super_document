# refresh_utf8format

## Location
src/fe_utils/print.c: 3691 - 3745

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
- : Pointer to printTableOpt structure containing Unicode formatting preferences including border_linestyle, header_linestyle, and column_linestyle settings

## Dependencies
- Functions called/Symbols referenced:
  - printTableOpt (parameter type)
  - printTextFormat (target structure type)
  - unicodeStyleBorderFormat (border style structure)
  - unicodeStyleRowFormat (row style structure)  
  - unicodeStyleColumnFormat (column style structure)
  - PRINT_RULE_TOP, PRINT_RULE_MIDDLE, PRINT_RULE_BOTTOM, PRINT_RULE_DATA (rule type constants)
- Called from (representative examples):
  - fmt (src/bin/psql/command.c:4627, 4642, 4657)
  - main (src/bin/psql/startup.c:181)

## Notes and Other Information
- Modifies the global  variable which is used throughout PostgreSQL's table printing system
- The function sets the format name to "unicode" to identify this as Unicode-based formatting
- All Unicode styles currently share the same newline and wrap formatting characters
- This function is part of PostgreSQL's frontend utilities print system, primarily used by psql for table output formatting
- The function maps three different line style dimensions (border, header, column) into a unified table format structure