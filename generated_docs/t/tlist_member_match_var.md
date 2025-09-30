# tlist_member_match_var

## Location
[src/backend/optimizer/util/tlist.c:102-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L102-L131)

## Overview
Searches a target list for a Var entry that matches the provided Var based on specific attributes rather than full structural equality.

## Definition

```c
static TargetEntry *
tlist_member_match_var(Var *var, List *targetlist)
```
## Detailed Description
The `tlist_member_match_var` function is a specialized version of `tlist_member` that performs a more lenient matching for Var nodes. Instead of using full structural equality, it matches Var nodes based only on varno, varattno, varlevelsup, and vartype. This is particularly useful when exact typmod matching cannot be guaranteed but the variables logically refer to the same column. The function is declared static, indicating it's an internal utility within the tlist.c module.

## Parameters / Member Variables
- `var`: The Var node to search for in the target list
- `targetlist`: A List of TargetEntry nodes to search through

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
- Called from (representative examples):
  - [apply_pathtarget_labeling_to_tlist](../a/apply_pathtarget_labeling_to_tlist.md)

## Notes and Other Information
- Only matches against TargetEntry expressions that are Var nodes
- Skips non-Var entries in the target list
- More flexible than full equality matching but maintains type safety by requiring vartype match
- Used when typmod differences should be ignored but logical column identity is preserved
- Internal function (static) used within the optimizer's target list utilities

## Simplified Source

```c
static TargetEntry *
tlist_member_match_var(Var *var, List *targetlist)
{
    ListCell *temp;

    // Search through each target list entry
    foreach(temp, targetlist)
    {
        TargetEntry *tlentry = (TargetEntry *) lfirst(temp);
        Var *tlvar = (Var *) tlentry->expr;

        // Skip non-Var entries
        if (!tlvar || !IsA(tlvar, Var))
            continue;

        // Check if this Var matches on key attributes
        if (var->varno == tlvar->varno &&
            var->varattno == tlvar->varattno &&
            var->varlevelsup == tlvar->varlevelsup &&
            var->vartype == tlvar->vartype)
            return tlentry;
    }

    // No match found
    return NULL;
}
```