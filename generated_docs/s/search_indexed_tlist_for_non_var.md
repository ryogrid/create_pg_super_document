# search_indexed_tlist_for_non_var

## Location
[src/backend/optimizer/plan/setrefs.c:2915-2954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2915-L2954)

## Overview
Searches for a non-Var/non-PlaceHolderVar expression in an indexed target list and returns a Var constructed to reference the matching target list item.

## Definition

```c
static Var *
search_indexed_tlist_for_non_var(Expr *node,
								 indexed_tlist *itlist, int newvarno)
```
## Detailed Description
This function searches through an indexed target list to find a non-Var, non-PlaceHolderVar expression that matches the provided node. When a match is found, it constructs and returns a new Var node that references the target list item containing the matching expression. The function uses tlist_member() to perform the matching.

The function includes an optimization where it avoids replacing simple Const nodes with Vars, since a Var is more expensive to execute than a Const. Additionally, replacing Consts could confuse executor components that expect to see simple Consts for specific purposes like dropped columns.

## Parameters / Member Variables
- : The expression node to search for in the indexed target list
- : The indexed target list structure to search within  
- : The varno value to assign to the constructed Var if a match is found

## Dependencies
- Functions called/Symbols referenced:
  - IsA
  - [tlist_member](../t/tlist_member.md)
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md)
- Data types used:
  - [Expr](../E/Expr.md)
  - [indexed_tlist](../i/indexed_tlist.md)
  - [TargetEntry](../T/TargetEntry.md)
  - [Const](../C/Const.md)
- Called from (representative examples):
  - fix_scan_list
  - [fix_join_expr_mutator](../f/fix_join_expr_mutator.md)
  - [fix_upper_expr_mutator](../f/fix_upper_expr_mutator.md)
  - [fix_windowagg_condition_expr_mutator](../f/fix_windowagg_condition_expr_mutator.md)

## Notes and Other Information
- Returns NULL if no matching expression is found in the indexed target list
- Returns NULL immediately if the input node is a simple Const, to avoid inefficient replacements
- The returned Var has varnosyn and varattnosyn set to 0, indicating it was never a plain Var
- It's recommended to check itlist->has_non_vars before calling this function, as it's a waste of time to call it otherwise
- Part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- Located in src/backend/optimizer/plan/setrefs.c at lines 2915-2954

## Simplified Source

```c
// Simplified version of search_indexed_tlist_for_non_var
static Var *
search_indexed_tlist_for_non_var(Expr *expression,
                                 indexed_tlist *target_list, int new_varno) {
    TargetEntry *matching_entry;

    // Skip simple constants - they're cheaper to execute than Vars
    if (IsA(expression, Const))
        return NULL;

    // Search for matching expression in target list
    matching_entry = tlist_member(expression, target_list->tlist);
    if (matching_entry) {
        // Create new Var referencing the target list entry
        Var *new_var = makeVarFromTargetEntry(new_varno, matching_entry);

        // Mark as synthetic (not originally a plain Var)
        new_var->varnosyn = 0;
        new_var->varattnosyn = 0;

        return new_var;
    }

    return NULL;  // No match found
}
```

Key simplifications made:
- Used more descriptive parameter names for clarity
- Added comments explaining the optimization for constants
- Simplified the matching logic while preserving the search behavior
- Focused on the core expression matching and Var creation logic