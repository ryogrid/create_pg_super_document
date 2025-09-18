# printTableCleanup

## Location
src/fe_utils/print.c: 3353 - 3402

## Overview
Frees all memory allocated to a printTableContent structure, making it ready for reuse or disposal.

## Definition


## Detailed Description
This function performs comprehensive memory cleanup for a printTableContent structure, freeing all dynamically allocated memory including cells, headers, footers, alignment information, and auxiliary data structures. The function handles selective cell cleanup based on the cellmustfree array, which tracks which individual cells were marked for automatic memory management.

The cleanup process traverses the singly-linked list of footers, freeing both the footer data strings and the footer nodes themselves. After cleanup, all pointers in the structure are set to NULL, making the structure safe to reuse with printTableInit() or safe to dispose of entirely.

## Parameters / Member Variables
- : Pointer to the printTableContent structure to clean up

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - unconstify (PostgreSQL utility macro for removing const qualifiers)
  - printTableFooter (structure type for footer cleanup)
- Called from (representative examples):
  - printCrosstab (crosstabview.c)
  - describeOneTableDetails (describe.c)
  - describeRoles (describe.c)
  - describePublications (describe.c)
  - printQuery (print.c)

## Notes and Other Information
- Handles selective cell cleanup using the cellmustfree array to determine which cells need freeing
- Uses unconstify macro to safely cast away const qualifiers when freeing cell strings
- Completely traverses and frees the footer linked list, including both data and node structures
- Sets all structure pointers to NULL after cleanup, preventing use-after-free errors
- The structure can be safely reused after cleanup by passing it to printTableInit()
- Essential for preventing memory leaks in long-running client applications like psql