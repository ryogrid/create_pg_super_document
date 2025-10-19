# printTableCleanup

## Location
[src/fe_utils/print.c:3353-3402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3353-L3402)

## Overview
Frees all memory allocated to a printTableContent structure, making it ready for reuse or disposal.

## Definition

```c
void
printTableCleanup(printTableContent *const content)
```
## Detailed Description
This function performs comprehensive memory cleanup for a printTableContent structure, freeing all dynamically allocated memory including cells, headers, footers, alignment information, and auxiliary data structures. The function handles selective cell cleanup based on the cellmustfree array, which tracks which individual cells were marked for automatic memory management.

The cleanup process traverses the singly-linked list of footers, freeing both the footer data strings and the footer nodes themselves. After cleanup, all pointers in the structure are set to NULL, making the structure safe to reuse with printTableInit() or safe to dispose of entirely.

## Parameters / Member Variables
- `content`: Pointer to the printTableContent structure to clean up
## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - unconstify (PostgreSQL utility macro for removing const qualifiers)
  - [printTableFooter](printTableFooter.md) (structure type for footer cleanup)
- Called from (representative examples):
  - [printCrosstab](printCrosstab.md) (crosstabview.c)
  - [describeOneTableDetails](../d/describeOneTableDetails.md) (describe.c)
  - [describeRoles](../d/describeRoles.md) (describe.c)
  - [describePublications](../d/describePublications.md) (describe.c)
  - [printQuery](printQuery.md) (print.c)

## Notes and Other Information
- Handles selective cell cleanup using the cellmustfree array to determine which cells need freeing
- Uses unconstify macro to safely cast away const qualifiers when freeing cell strings
- Completely traverses and frees the footer linked list, including both data and node structures
- Sets all structure pointers to NULL after cleanup, preventing use-after-free errors
- The structure can be safely reused after cleanup by passing it to printTableInit()
- Essential for preventing memory leaks in long-running client applications like psql

## Simplified Source

```c
void printTableCleanup(printTableContent *content) {
    // Free individual cells that were marked for cleanup
    if (content->cellmustfree) {
        uint64 total_cells = content->ncolumns * content->nrows;
        for (uint64 i = 0; i < total_cells; i++) {
            if (content->cellmustfree[i])
                free(unconstify(char *, content->cells[i]));
        }
        free(content->cellmustfree);
    }

    // Free main data arrays
    free(content->headers);
    free(content->cells);
    free(content->aligns);

    // Clear all pointers to NULL
    content->opt = NULL;
    content->title = NULL;
    content->headers = NULL;
    content->cells = NULL;
    content->aligns = NULL;
    content->header = NULL;
    content->cell = NULL;
    content->align = NULL;

    // Free footer linked list
    if (content->footers) {
        for (content->footer = content->footers; content->footer;) {
            printTableFooter *f = content->footer;
            content->footer = f->next;
            free(f->data);
            free(f);
        }
    }
    content->footers = NULL;
    content->footer = NULL;
}
```