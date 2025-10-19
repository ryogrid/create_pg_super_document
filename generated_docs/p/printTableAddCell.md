# printTableAddCell

## Location
[src/fe_utils/print.c:3260-3309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3260-L3309)

## Overview
Adds a cell to a table content structure used for formatting tabular output in PostgreSQL client utilities.

## Definition

```c
void
printTableAddCell(printTableContent *const content, char *cell,
				  const bool translate, const bool mustfree)
```
## Detailed Description
This function adds a single cell to a printTableContent structure, which is used to build tabular output for PostgreSQL client utilities like psql. The function handles cell validation, optional translation through gettext, and memory management. It performs bounds checking to ensure the cell count doesn't exceed the allocated table dimensions and validates the cell content using multibyte character validation.

The function maintains an internal counter of added cells and can optionally mark cells for automatic cleanup. When translation is enabled and the translate parameter is true, the cell content is passed through gettext for internationalization.

## Parameters / Member Variables
- `content`: Pointer to the printTableContent structure that will contain the cell
- `*cell`: The string content to add as a cell (not duplicated, must remain valid for the table's lifetime)
- `translate`: If true, the cell content will be passed through gettext for translation
- `mustfree`: If true, the cell string will be automatically freed during printTableCleanup()
## Dependencies
- Functions called/Symbols referenced:
  - [mbvalidate](../m/mbvalidate.md) (multibyte character validation)
  - [pg_malloc0](pg_malloc0.md) (PostgreSQL memory allocation)
  - gettext (_ macro, when ENABLE_NLS is defined)
- Called from (representative examples):
  - [describeOneTableDetails](../d/describeOneTableDetails.md) (multiple locations in describe.c)
  - [describeRoles](../d/describeRoles.md) (describe.c)
  - [describePublications](../d/describePublications.md) (describe.c)
  - [printQuery](printQuery.md) (print.c)

## Notes and Other Information
- Cells are not duplicated by this function; the caller must ensure the cell string remains valid
- Translation of strings that are marked for automatic freeing (mustfree=true) is not supported
- The function will exit with EXIT_FAILURE if the total cell count is exceeded
- Memory for tracking which cells need to be freed is allocated lazily when the first mustfree=true cell is added
- The function uses mbvalidate to ensure proper multibyte character encoding

## Simplified Source

```c
void
printTableAddCell(printTableContent *const content, char *cell,
                  const bool translate, const bool mustfree)
{
    // Calculate total cells and check bounds
    uint64 total_cells = (uint64) content->ncolumns * content->nrows;
    if (content->cellsadded >= total_cells) {
        fprintf(stderr, _("Cannot add cell to table content: total cell count of %lld exceeded.\n"),
                (long long int) total_cells);
        exit(EXIT_FAILURE);
    }

    // Validate multibyte encoding of cell content
    *content->cell = (char *) mbvalidate((unsigned char *) cell,
                                        content->opt->encoding);

#ifdef ENABLE_NLS
    // Translate cell if requested and NLS is enabled
    if (translate)
        *content->cell = _(*content->cell);
#endif

    // Handle memory management tracking if needed
    if (mustfree) {
        // Allocate mustfree tracking array lazily on first use
        if (content->cellmustfree == NULL)
            content->cellmustfree = pg_malloc0((total_cells + 1) * sizeof(bool));

        content->cellmustfree[content->cellsadded] = true;
    }

    // Advance to next cell position
    content->cell++;
    content->cellsadded++;
}
```