# print_latex_vertical

## Location
src/fe_utils/print.c: 2717 - 2809

## Overview
Prints table data in LaTeX vertical format where each record is displayed as field-value pairs in a two-column layout with headers and data side by side.

## Definition


## Detailed Description
This function formats tabular data in a vertical LaTeX layout using a two-column table structure where the left column contains field names (headers) and the right column contains the corresponding values. Each record is preceded by a "Record N" header that spans both columns. The function uses a standard tabular environment with configurable borders and supports optional titles displayed in a centered environment above the table. Record numbering continues from previous tables using prior_records.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing table data, headers, border options, titles, footers, and formatting metadata
- : File stream where the generated vertical LaTeX table code will be written

## Dependencies
- Functions called/Symbols referenced:
  - [latex_escaped_print](../l/latex_escaped_print.md) (for escaping special LaTeX characters in headers and content)
  - [printTableContent](printTableContent.md) (data structure)
  - [printTableFooter](printTableFooter.md) (data structure)
  - cancel_pressed (global variable for interrupt handling)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function, called for both regular vertical and when converting from other formats)

## Notes and Other Information
- This is a static function within print.c used internally for vertical LaTeX table formatting
- Uses a two-column tabular layout: left column for field names, right column for values
- Supports 3 border levels: 0 (no borders), 1 (center divider), 2 (full borders around entire table)
- Border level is clamped to maximum of 2 (unlike other LaTeX functions that support level 3)
- Record headers use \multicolumn{2}{c}{} or \multicolumn{2}{|c|}{} to span both columns
- Record numbering starts from cont->opt->prior_records + 1 for pagination support
- Uses \textit{} for record headers and field names formatting
- Titles are centered above the table using \begin{center}...\end{center}
- Footers are printed with line breaks and no indentation after table completion
- Handles cancellation via cancel_pressed global variable for responsive interruption