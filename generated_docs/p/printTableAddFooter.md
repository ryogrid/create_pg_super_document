# printTableAddFooter

## Location
src/fe_utils/print.c: 3310 - 3334

## Overview
Adds a footer to a table content structure as part of a singly-linked list, with automatic memory management for the footer string.

## Definition


## Detailed Description
This function adds a footer to a printTableContent structure by creating a new printTableFooter node and adding it to the end of a singly-linked list. The function automatically duplicates the footer string using pg_strdup(), so the caller doesn't need to maintain the original string. Unlike headers and cells, footers are never automatically translated by this function - translation must be done by the caller before calling this function.

The design assumes that footers are typically composed of individually translated components rather than being translated as complete strings, which is why the translation behavior differs from other table elements.

## Parameters / Member Variables
- : Pointer to the printTableContent structure that will contain the footer
- : The string content to add as a footer (will be duplicated via pg_strdup)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0 (PostgreSQL memory allocation)
  - [pg_strdup](pg_strdup.md) (PostgreSQL string duplication)
  - [printTableFooter](printTableFooter.md) (structure type)
- Called from (representative examples):
  - [describeOneTableDetails](../d/describeOneTableDetails.md) (extensively used in describe.c)
  - [add_tablespace_footer](../a/add_tablespace_footer.md) (describe.c)
  - [addFooterToPublicationDesc](../a/addFooterToPublicationDesc.md) (describe.c)
  - [printTableSetFooter](printTableSetFooter.md) (print.c)
  - [printQuery](printQuery.md) (print.c)

## Notes and Other Information
- Footer strings are automatically duplicated, so the original string does not need to persist
- Footers are stored as a singly-linked list, allowing multiple footers per table
- Translation must be performed by the caller before passing the footer to this function
- The function maintains both a footers pointer (to the first footer) and footer pointer (to the last footer) for efficient list management
- Memory for footers is automatically managed and will be cleaned up by printTableCleanup()