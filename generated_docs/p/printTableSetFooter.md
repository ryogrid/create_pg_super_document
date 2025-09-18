# printTableSetFooter

## Location
src/fe_utils/print.c: 3335 - 3352

## Overview
Modifies the content of the last-added footer in a table content structure, or adds a new footer if none exists.

## Definition


## Detailed Description
This function provides a way to modify the content of the most recently added footer in a printTableContent structure. If a footer already exists, it frees the current content of the last footer and replaces it with the new content. If no footers exist yet, it delegates to printTableAddFooter to create a new footer.

The function automatically manages memory by freeing the old footer content and duplicating the new footer string using pg_strdup(), ensuring the caller doesn't need to maintain the original string.

## Parameters / Member Variables
- : Pointer to the printTableContent structure containing the footer to modify
- : The new string content for the footer (will be duplicated via pg_strdup)

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - [pg_strdup](pg_strdup.md) (PostgreSQL string duplication)
  - [printTableAddFooter](printTableAddFooter.md) (fallback when no footers exist)
- Called from (representative examples):
  - [add_tablespace_footer](../a/add_tablespace_footer.md) (describe.c)

## Notes and Other Information
- Operates on the last-added footer only, not all footers
- Automatically handles memory management for both old and new footer content
- Falls back to printTableAddFooter when no footers exist, maintaining consistent behavior
- Footer strings are automatically duplicated, so the original string does not need to persist
- Primarily used for updating dynamic footer content that may change based on query results or user preferences