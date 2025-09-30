# ExpandColumnRefStar

## Location
[src/backend/parser/parse_target.c:1120-1160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1120-L1160)

## Overview
Transforms "foo.*" expressions into a list of individual column expressions or target list entries, handling both bare "*" and qualified "relation.*" syntax.

## Definition

```c
enum
		{
			CRSERR_NO_RTE,
			CRSERR_WRONG_DB,
			CRSERR_TOO_MANY
		}			crserr = CRSERR_NO_RTE;
```
## Detailed Description
This function expands star expressions in SQL queries, handling two distinct cases:

1. **Bare asterisk (*)**: When numnames == 1, it represents a bare "*" which expands to all tables in the current namespace using ExpandAllTables().

2. **Qualified asterisk (relation.*)**: When numnames > 1, it represents a qualified reference like "emp.*" which expands to all columns of a specific relation. The function performs namespace resolution to locate the target relation, supporting:
   - 2-part names: "relation.*"
   - 3-part names: "schema.relation.*" 
   - 4-part names: "database.schema.relation.*" (with database validation)

The function includes sophisticated error handling for ambiguous references, missing relations, cross-database references, and improper qualified names. It also integrates with parser hooks (PreParseColumnRefHook and PostParseColumnRefHook) to allow custom column resolution behavior.

A key design consideration is permission handling: the function avoids marking the whole row as requiring SELECT permission when expanding to individual columns, ensuring that columns added later don't inherit unnecessary permissions.

## Parameters / Member Variables
- : ParseState structure containing parsing context and namespace information
- : ColumnRef node representing the star expression to be expanded
- : Boolean flag indicating whether to create TargetEntry nodes (true for SELECT lists) or bare expressions (true for ROW()/VALUES() constructs)

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - [ExpandAllTables](ExpandAllTables.md)
  - [refnameNamespaceItem](../r/refnameNamespaceItem.md)
  - strVal
  - linitial/lsecond/lthird
  - [get_database_name](../g/get_database_name.md)
  - [ExpandRowReference](ExpandRowReference.md)
  - [ExpandSingleTable](ExpandSingleTable.md)
  - [errorMissingRTE](../e/errorMissingRTE.md)
  - [makeRangeVar](../m/makeRangeVar.md)
  - [NameListToString](../N/NameListToString.md)
- Called from (representative examples):
  - [transformTargetList](../t/transformTargetList.md) (src/backend/parser/parse_target.c:153)
  - [transformExpressionList](../t/transformExpressionList.md) (src/backend/parser/parse_target.c:243)

## Notes and Other Information
- The function defines local enum values (CRSERR_NO_RTE, CRSERR_WRONG_DB, CRSERR_TOO_MANY) for error classification
- Cross-database references are explicitly not supported and generate an error
- The function supports parser hooks for custom column reference resolution
- Permission tracking is carefully handled to avoid unnecessary whole-row SELECT permissions
- The Assert(make_target_entry) for bare "*" reflects a grammar constraint that bare "*" only appears at SELECT top level
- The function is marked static, indicating it's an internal helper within parse_target.c

## Simplified Source

```c
static List *
ExpandColumnRefStar(ParseState *pstate, ColumnRef *cref,
                    bool make_target_entry)
{
    List *fields = cref->fields;
    int numnames = list_length(fields);

    if (numnames == 1) {
        // Bare '*' - expand all tables
        Assert(make_target_entry);
        return ExpandAllTables(pstate, cref->location);
    } else {
        // Qualified '*' - expand specific relation (relation.*)
        char *nspname = NULL;
        char *relname = NULL;
        ParseNamespaceItem *nsitem = NULL;
        int levels_up;

        // Parse the qualified name components
        switch (numnames) {
            case 2:
                // relation.*
                relname = strVal(linitial(fields));
                break;
            case 3:
                // schema.relation.*
                nspname = strVal(linitial(fields));
                relname = strVal(lsecond(fields));
                break;
            case 4:
                // database.schema.relation.*
                // Check database name matches current database
                char *catname = strVal(linitial(fields));
                if (strcmp(catname, get_database_name(MyDatabaseId)) != 0)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                   errmsg("cross-database references are not implemented")));
                nspname = strVal(lsecond(fields));
                relname = strVal(lthird(fields));
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                               errmsg("improper qualified name (too many dotted names)")));
                break;
        }

        // Find the relation in namespace
        nsitem = refnameNamespaceItem(pstate, nspname, relname,
                                      cref->location, &levels_up);
        if (nsitem == NULL) {
            errorMissingRTE(pstate, makeRangeVar(nspname, relname, cref->location));
        }

        // Expand the relation's columns
        return ExpandSingleTable(pstate, nsitem, levels_up, cref->location,
                                 make_target_entry);
    }
}
```