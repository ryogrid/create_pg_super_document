# find_var_for_subquery_tle

## Location
[src/backend/optimizer/path/pathkeys.c:1249-1291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1249-L1291)

## Overview
Determines if a subquery target list entry is visible to the outer query and returns the corresponding Var reference if available.

## Definition

```c
static Var *
find_var_for_subquery_tle(RelOptInfo *rel, TargetEntry *tle)
```
## Detailed Description
This function serves as a bridge between subquery internal representations and outer query visibility. It checks whether a specific target list entry from a subquery will be accessible to the outer query by searching the relation's target expressions.

The function performs several validation steps:
1. Immediately returns NULL for resjunk entries, as these are internal-only and not visible to outer queries
2. Searches through the relation's target list expressions to find a matching Var
3. Validates that found variables reference the correct relation and attribute number
4. Returns a copy of the matching Var to ensure memory safety

This is crucial for pathkey conversion, as it prevents the creation of pathkeys that reference values not actually available above the subquery level.

## Parameters / Member Variables
- : RelOptInfo representing the subquery relation in the outer query context
- : TargetEntry from the subquery's target list to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (to create a safe copy of the Var node)
- Called from (representative examples):
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md) (multiple calls for pathkey conversion)

## Notes and Other Information
- This is a static helper function used internally within the pathkeys module
- Essential for ensuring pathkey validity across subquery boundaries
- Resjunk entries are automatically excluded as they're not visible to outer queries
- Returns a copied Var node to prevent memory management issues
- Part of PostgreSQL's subquery optimization system for maintaining proper variable references

## Simplified Source

```c
static Var *find_var_for_subquery_tle(RelOptInfo *rel, TargetEntry *tle) {
    ListCell *lc;

    // Resjunk entries are not visible to outer queries
    if (tle->resjunk)
        return NULL;

    // Search the relation's target expressions for a matching Var
    foreach(lc, rel->reltarget->exprs) {
        Var *var = (Var *) lfirst(lc);

        // Skip non-Var expressions (placeholders)
        if (!IsA(var, Var))
            continue;

        Assert(var->varno == rel->relid);

        // Check if this Var references the target list entry
        if (var->varattno == tle->resno)
            return copyObject(var);  // Return a safe copy
    }

    return NULL;  // No matching Var found
}
```