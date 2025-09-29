# canonicalize_qual

## Location
[src/backend/optimizer/prep/prepqual.c:293-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepqual.c#L293-L322)

## Overview
Converts a qualification expression (WHERE clause, JOIN/ON clause, or CHECK constraint) to the most useful form by removing redundancy and applying optimizations at the top-level structure.

## Definition

```c
structure;
```
## Detailed Description
The  function takes a qualification expression and transforms it into a more optimized form. Despite its name suggesting conversion to canonical form, the function actually performs practical optimizations rather than enforcing a specific canonical structure like AND-of-ORs or OR-of-ANDs.

The function operates specifically on top-level WHERE clauses, JOIN/ON clauses, and CHECK constraints. It assumes the input has already been processed by  and thus has AND/OR flatness. The main transformation performed is pulling up redundant subclauses in OR-of-AND structures and removing NULL constants from the top-level structure.

The function only works within the top-level AND/OR structure and does not recurse into deeper levels, as such deeper analysis is not beneficial for the intended optimizations.

## Parameters / Member Variables
- : The qualification expression to canonicalize (can be NULL for quick exit)
- : Boolean flag indicating whether this is a CHECK constraint (true) or WHERE/JOIN clause (false)

## Dependencies
- Functions called/Symbols referenced:
  -  - performs the main optimization work of finding and handling duplicate OR clauses
- Called from (representative examples):
  -  (src/backend/catalog/partition.c:389)
  -  (src/backend/commands/copy.c:150)
  -  (src/backend/commands/tablecmds.c:18385)
  -  (src/backend/optimizer/plan/planner.c:1209)
  -  (src/backend/optimizer/plan/subselect.c:1697)
  -  (src/backend/optimizer/util/plancat.c:1317)
  -  (src/backend/utils/cache/relcache.c:5198)

## Notes and Other Information
- The function name is a historical holdover from when it attempted to force expressions into canonical AND-of-ORs or OR-of-ANDs form
- Requires input to already have AND/OR flatness (should be preprocessed by )
- Should NOT be called on expressions that are not top-level WHERE, JOIN/ON, or CHECK constraints
- Returns NULL if input qualification is NULL
- Asserts that input is not in implicit-AND format (should not be a List)
- Part of PostgreSQL's query preprocessing and optimization pipeline
- Focuses on practical optimization benefits rather than theoretical canonical forms

## Simplified Source

```c
Expr *
canonicalize_qual(Expr *qual, bool is_check)
{
    // Quick exit for empty qualification
    if (qual == NULL)
        return NULL;

    // Ensure input is not in implicit-AND format
    Assert(!IsA(qual, List));

    // Remove redundant subclauses in OR-of-AND trees
    // and eliminate NULL constants from top-level structure
    Expr *optimized_qual = find_duplicate_ors(qual, is_check);

    return optimized_qual;
}
```