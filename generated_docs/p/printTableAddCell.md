# printTableAddCell

## Location
src/fe_utils/print.c: 3260 - 3309

## Overview
Adds a cell to a table content structure used for formatting tabular output in PostgreSQL client utilities.

## Definition


## Detailed Description
This function adds a single cell to a printTableContent structure, which is used to build tabular output for PostgreSQL client utilities like psql. The function handles cell validation, optional translation through gettext, and memory management. It performs bounds checking to ensure the cell count doesn't exceed the allocated table dimensions and validates the cell content using multibyte character validation.

The function maintains an internal counter of added cells and can optionally mark cells for automatic cleanup. When translation is enabled and the translate parameter is true, the cell content is passed through gettext for internationalization.

## Parameters / Member Variables
- : Pointer to the printTableContent structure that will contain the cell
- : The string content to add as a cell (not duplicated, must remain valid for the table's lifetime)
- : If true, the cell content will be passed through gettext for translation
- : If true, the cell string will be automatically freed during printTableCleanup()

## Dependencies
- Functions called/Symbols referenced:
  - mbvalidate (multibyte character validation)
  - pg_malloc0 (PostgreSQL memory allocation)
  - gettext (_ macro, when ENABLE_NLS is defined)
- Called from (representative examples):
  - describeOneTableDetails (multiple locations in describe.c)
  - describeRoles (describe.c)
  - describePublications (describe.c)
  - printQuery (print.c)

## Notes and Other Information
- Cells are not duplicated by this function; the caller must ensure the cell string remains valid
- Translation of strings that are marked for automatic freeing (mustfree=true) is not supported
- The function will exit with EXIT_FAILURE if the total cell count is exceeded
- Memory for tracking which cells need to be freed is allocated lazily when the first mustfree=true cell is added
- The function uses mbvalidate to ensure proper multibyte character encoding