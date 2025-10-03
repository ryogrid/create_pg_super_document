# ExpandAllTables

## Location
[src/backend/parser/parse_target.c:1293-1344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1293-L1344)

## Overview
Transforms a bare "*" in the target list into a list of target list entries by expanding all relations visible for unqualified column name access.

## Definition

```c
static List *
ExpandAllTables(ParseState *pstate, int location)
```
## Detailed Description
This function implements the expansion logic for bare asterisk ("*") expressions in SELECT statements. It iterates through all namespace items in the current parsing context (pstate->p_namespace) and expands each relation that has columns visible for unqualified access.

The function employs several important filtering criteria:
- Only considers relations where p_cols_visible is true, excluding table-only entries
- Skips qualified-name-only entries to avoid including input tables of aliasless JOINs, NEW/OLD pseudo-entries, and similar constructs
- Asserts that no lateral-only items should exist when parsing target lists

For each qualifying relation, the function calls expandNSItemAttrs() to generate the actual target list entries for all columns in that relation. The results are concatenated into a single target list.

The function includes a special validation case for "SELECT *;" (a SELECT with no FROM clause), which is explicitly prohibited and generates a syntax error. This check is performed by tracking whether any p_cols_visible tables were found, rather than simply checking if the result is empty, to accommodate zero-column tables.

## Parameters / Member Variables
- `*pstate`: ParseState structure containing the current parsing context and namespace information
- `location`: Source location information for error reporting purposes
## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md) (data structure access)
  - [list_concat](../l/list_concat.md)
  - [expandNSItemAttrs](../e/expandNSItemAttrs.md)
  - ereport
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [ExpandColumnRefStar](ExpandColumnRefStar.md) (src/backend/parser/parse_target.c:1137)

## Notes and Other Information
- The function is marked static, indicating it's an internal helper within parse_target.c
- Referenced relations and columns are automatically marked as requiring SELECT access permissions
- The function distinguishes between truly empty result sets and the invalid "SELECT *;" case
- The Assert(!nsitem->p_lateral_only) reflects a parser constraint about lateral references in target lists
- The function supports zero-column tables, allowing "SELECT * FROM zero_column_table" to succeed
- Error messages include precise location information for better user experience
- This function is specifically designed for bare "*" expansion, while qualified "relation.*" expansion is handled by ExpandColumnRefStar()

## Simplified Source

```c
static List *
ExpandAllTables(ParseState *pstate, int location)
{
    List *target = NIL;
    bool found_table = false;
    ListCell *l;

    // Iterate through all namespace items
    foreach(l, pstate->p_namespace)
    {
        ParseNamespaceItem *nsitem = (ParseNamespaceItem *) lfirst(l);

        // Skip table-only items (no visible columns)
        if (!nsitem->p_cols_visible)
            continue;

        // Should not have lateral-only items in target list
        Assert(!nsitem->p_lateral_only);

        // Remember we found a table with visible columns
        found_table = true;

        // Expand all attributes of this namespace item
        target = list_concat(target,
                           expandNSItemAttrs(pstate, nsitem, 0, true, location));
    }

    // Check for invalid "SELECT *;" with no tables
    if (!found_table)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("SELECT * with no tables specified is not valid"),
                 parser_errposition(pstate, location)));

    return target;
}
```