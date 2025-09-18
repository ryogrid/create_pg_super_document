# printTableContent

## Location
src/include/fe_utils/print.h: 163 - 181

## Overview
The printTableContent struct holds all the information that will be displayed by printTable(), serving as a comprehensive data container for tabular output formatting in PostgreSQL's frontend utilities.

## Definition


## Detailed Description
The printTableContent structure is a central data structure in PostgreSQL's table printing system, designed to hold all necessary information for rendering tabular data in various output formats. This struct supports dynamic construction of tables by providing pointers to track the current position for adding headers, cells, footers, and alignment specifications. It manages memory allocation tracking through the cellmustfree array, allowing for efficient cleanup of dynamically allocated cell content. The structure supports both static and dynamic table construction, making it suitable for displaying query results, system information, and other tabular data in psql and other PostgreSQL frontend utilities.

## Parameters / Member Variables
- : Pointer to printTableOpt structure containing formatting options and display preferences
- : Optional title string for the table (may be NULL)
- : Number of columns in the table, specified during initialization
- : Number of rows in the table, specified during initialization
- : NULL-terminated array of header strings for column titles
- : Pointer to the last added header, used for incremental header construction
- : NULL-terminated array containing all cell content strings in row-major order
- : Pointer to the last added cell, facilitating sequential cell addition
- : Counter tracking the total number of cells added to the table
- : Boolean array indicating which cells require memory deallocation
- : Pointer to the first footer in a linked list of table footers
- : Pointer to the last added footer for efficient footer appending
- : Array of alignment specifiers ('l' for left, 'r' for right) for each column
- : Pointer to the last added alignment specifier

## Dependencies
- Functions called/Symbols referenced:
  - [printTableOpt](printTableOpt.md)
  - [printTableFooter](printTableFooter.md)
- Called from (representative examples):
  - [printTableInit](printTableInit.md)
  - [printTableAddHeader](printTableAddHeader.md)
  - [printTableAddCell](printTableAddCell.md)
  - [printTableAddFooter](printTableAddFooter.md)
  - [printTable](printTable.md)
  - [printQuery](printQuery.md)
  - [print_aligned_text](print_aligned_text.md)
  - [print_html_text](print_html_text.md)
  - [print_csv_text](print_csv_text.md)

## Notes and Other Information
This structure is extensively used throughout PostgreSQL's frontend utilities, particularly in psql for displaying query results and system information. The design allows for incremental table construction while maintaining efficient access to formatting options. Memory management is handled through the cellmustfree array, ensuring proper cleanup of dynamically allocated content. The structure supports various output formats including aligned text, HTML, CSV, LaTeX, and others through the associated printing functions.