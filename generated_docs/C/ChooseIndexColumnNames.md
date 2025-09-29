# ChooseIndexColumnNames

## Location
[src/backend/commands/indexcmds.c:2632-2692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2632-L2692)

## Overview
ChooseIndexColumnNames generates unique column names for an index by processing IndexElem nodes and resolving name conflicts by appending numeric suffixes when necessary.

## Definition

```c
static List *
ChooseIndexColumnNames(const List *indexElems)
```
## Detailed Description
ChooseIndexColumnNames takes a list of IndexElem nodes representing the columns/expressions in an index and produces a list of unique string names for those columns. It handles three types of column specifications:
1. Explicitly named columns (using indexcolname)
2. Simple column references (using the column's name)
3. Expression-based columns (defaulting to "expr")

When name conflicts arise, the function automatically resolves them by appending numeric suffixes (e.g., "col", "col1", "col2"). The function also ensures generated names comply with PostgreSQL's NAMEDATALEN limit by truncating the original name when necessary to make room for the numeric suffix.

## Parameters / Member Variables
- : A List of IndexElem nodes representing the columns/expressions that will comprise the index

## Dependencies
- Functions called/Symbols referenced:
  - [IndexElem](../I/IndexElem.md) (structure representing an index column/expression)
  - NAMEDATALEN (PostgreSQL's maximum name length constant)
  - [pg_mbcliplen](../p/pg_mbcliplen.md) (multibyte-aware string clipping function)
  - lfirst (list iteration macro)
  - foreach (list iteration macro)
  - [lappend](../l/lappend.md) (list append function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL's string duplication function)
  - strcmp (string comparison function)
- Called from:
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:814)

## Notes and Other Information
- This is a static function internal to indexcmds.c
- Returns a List of plain char* strings, not String nodes
- The conflict resolution algorithm is simple: append increasing integers starting from 1
- Uses pg_mbcliplen to handle multibyte character encodings properly when truncating names
- Ensures uniqueness within the scope of a single index's columns
- The "expr" default name is used for expression-based index columns that don't have explicit names
- All returned strings are palloc'd and must be freed by the caller
- Names are generated in the same order as the input IndexElem list

## Simplified Source

```c
static List *ChooseIndexColumnNames(const List *indexElems) {
    List *result = NIL;
    ListCell *lc;

    foreach(lc, indexElems) {
        IndexElem *ielem = (IndexElem *) lfirst(lc);
        const char *origname;
        const char *curname;

        // Get the preliminary name from IndexElem
        if (ielem->indexcolname)
            origname = ielem->indexcolname;    // Explicit name
        else if (ielem->name)
            origname = ielem->name;            // Column name
        else
            origname = "expr";                 // Default for expressions

        // Resolve name conflicts by appending numbers
        curname = origname;
        for (int i = 1; ; i++) {
            // Check if current name conflicts with existing names
            ListCell *lc2;
            bool conflict = false;
            foreach(lc2, result) {
                if (strcmp(curname, (char *) lfirst(lc2)) == 0) {
                    conflict = true;
                    break;
                }
            }
            if (!conflict)
                break;  // Found unique name

            // Generate new name with numeric suffix
            char nbuf[32];
            char buf[NAMEDATALEN];
            sprintf(nbuf, "%d", i);

            // Ensure name fits within NAMEDATALEN limit
            int nlen = pg_mbcliplen(origname, strlen(origname),
                                   NAMEDATALEN - 1 - strlen(nbuf));
            memcpy(buf, origname, nlen);
            strcpy(buf + nlen, nbuf);
            curname = buf;
        }

        // Add the unique name to result list
        result = lappend(result, pstrdup(curname));
    }
    return result;
}
```