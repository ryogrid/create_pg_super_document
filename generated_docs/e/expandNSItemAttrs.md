# expandNSItemAttrs

## Location
[src/backend/parser/parse_relation.c:3187-3252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3187-L3252)

## Overview
This function is the workhorse for "*" expansion in PostgreSQL's parser, producing a list of target entries for all attributes of a given namespace item.

## Definition

```c
List *
expandNSItemAttrs(ParseState *pstate, ParseNamespaceItem *nsitem,
				  int sublevels_up, bool require_col_privs, int location)
```
## Detailed Description
The  function expands a "*" reference into a list of  nodes representing all visible columns of a table or other relation. It serves as the core implementation for SELECT * operations and similar wildcard expansions in SQL queries. The function handles permission checking, assigns result numbers to target list entries, and marks columns as requiring SELECT access when requested.

The function works by first calling  to get the list of variables and column names, then creates  structures for each column with appropriate result numbers assigned from .

## Parameters / Member Variables
- `*pstate`: Parse state containing context information including the next available result number
- `*nsitem`: ParseNamespaceItem representing the table/relation whose attributes should be expanded
- `sublevels_up`: Number of query levels up to look for the relation (for nested queries)
- `require_col_privs`: Boolean flag indicating whether to mark columns as requiring SELECT privileges
- `location`: Source location in the query for error reporting purposes
## Dependencies
- Functions called/Symbols referenced:
  - [expandNSItemVars](expandNSItemVars.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [markVarForSelectPriv](../m/markVarForSelectPriv.md)
  - forboth (macro)
  - RTE_RELATION
  - ACL_SELECT
- Called from (representative examples):
  - [transformValuesClause](../t/transformValuesClause.md)
  - [ExpandAllTables](../E/ExpandAllTables.md)
  - [ExpandSingleTable](../E/ExpandSingleTable.md)

## Notes and Other Information
- The function automatically handles permission checking for table access, marking the relation as requiring ACL_SELECT permission
- For relations (not joins), it ensures SELECT permission is granted even if the table has zero columns
- The function maintains assertion checks to ensure the names and variables lists have matching lengths
- [Result](../R/Result.md) numbers are automatically assigned and incremented via 
- This function is essential for implementing SQL's "*" wildcard functionality in SELECT statements

## Simplified Source

```c
List *
expandNSItemAttrs(ParseState *pstate, ParseNamespaceItem *nsitem,
                  int sublevels_up, bool require_col_privs, int location)
{
    RangeTblEntry *rte = nsitem->p_rte;
    RTEPermissionInfo *perminfo = nsitem->p_perminfo;
    List *names, *vars;
    ListCell *name, *var;
    List *te_list = NIL;

    // Get variables and names for all attributes
    vars = expandNSItemVars(pstate, nsitem, sublevels_up, location, &names);

    // Mark table as requiring SELECT access for relations
    if (rte->rtekind == RTE_RELATION)
    {
        Assert(perminfo != NULL);
        perminfo->requiredPerms |= ACL_SELECT;
    }

    // Create target entries for each column
    forboth(name, names, var, vars)
    {
        char *label = strVal(lfirst(name));
        Var *varnode = (Var *) lfirst(var);
        TargetEntry *te;

        te = makeTargetEntry((Expr *) varnode,
                            (AttrNumber) pstate->p_next_resno++,
                            label,
                            false);
        te_list = lappend(te_list, te);

        // Mark column for SELECT privilege if requested
        if (require_col_privs)
            markVarForSelectPriv(pstate, varnode);
    }

    Assert(name == NULL && var == NULL);  // Ensure lists same length

    return te_list;
}
```