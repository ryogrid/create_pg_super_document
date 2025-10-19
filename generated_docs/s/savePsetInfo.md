# savePsetInfo

## Location
[src/bin/psql/command.c:5086-5121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5086-L5121)

## Overview
Creates a deep copy of a printQueryOpt structure, allocating memory and duplicating all dynamically allocated string fields to ensure the copy is independent of the original.

## Definition

```c
printQueryOpt *savePsetInfo(const printQueryOpt *popt)
```
## Detailed Description
The savePsetInfo function performs a complete deep copy of a printQueryOpt structure, which contains PostgreSQL query result printing options. It first performs a flat copy of all scalar fields using memcpy, then selectively duplicates dynamically allocated string members to prevent sharing of memory between the original and the copy. This ensures that modifications to either structure won't affect the other, and both can be independently freed.

The function is specifically designed for psql's printing system and includes assertions to verify that certain fields (footers and translate_columns) are never set in psql's context, as these would require additional duplication logic.

## Parameters / Member Variables
- `popt`: Pointer to the source printQueryOpt structure to be copied

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (for memory allocation)
  - [pg_strdup](../p/pg_strdup.md) (for string duplication)
  - memcpy (for structure copying)
  - [printQueryOpt](../p/printQueryOpt.md) (structure type)
- Called from (representative examples):
  - [exec_command_g](../e/exec_command_g.md)
  - [process_command_g_options](../p/process_command_g_options.md)

## Notes and Other Information
- The function handles NULL string pointers gracefully by only duplicating non-NULL strings
- Contains assertions that footers and translate_columns are always NULL in psql context
- The topt.line_style field points to constant data that doesn't need duplication
- Memory allocated by this function should be freed using the corresponding restorePsetInfo function
- Specific to psql's printing system and may not be suitable for general-purpose printQueryOpt copying

## Simplified Source

```c
printQueryOpt *savePsetInfo(const printQueryOpt *popt) {
    printQueryOpt *save;

    // Allocate memory for the copy
    save = (printQueryOpt *) pg_malloc(sizeof(printQueryOpt));

    // Flat-copy all scalar fields
    memcpy(save, popt, sizeof(printQueryOpt));

    // Duplicate dynamically allocated string fields
    if (popt->topt.fieldSep.separator)
        save->topt.fieldSep.separator = pg_strdup(popt->topt.fieldSep.separator);
    if (popt->topt.recordSep.separator)
        save->topt.recordSep.separator = pg_strdup(popt->topt.recordSep.separator);
    if (popt->topt.tableAttr)
        save->topt.tableAttr = pg_strdup(popt->topt.tableAttr);
    if (popt->nullPrint)
        save->nullPrint = pg_strdup(popt->nullPrint);
    if (popt->title)
        save->title = pg_strdup(popt->title);

    // Assert that unused fields are NULL (psql-specific invariant)
    Assert(popt->footers == NULL);
    Assert(popt->translate_columns == NULL);

    return save;
}
```