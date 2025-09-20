# find_dependent_phvs

## Location
[src/backend/optimizer/prep/prepjointree.c:3876-3900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3876-L3900)

## Overview
Determines whether the query parse tree contains PlaceHolderVars (PHVs) that depend on a specific relation variable.

## Definition

```c
static bool
find_dependent_phvs(PlannerInfo *root, int varno)
```
## Detailed Description
This function searches through the query parse tree to find PlaceHolderVars that have dependencies on a specific relation identified by . It performs an optimization check early on by examining if any PlaceHolderVars exist in the query at all (via ). If none exist, it immediately returns false to avoid unnecessary traversal.

The function uses a tree walker pattern to traverse both the main query parse tree () and any append relation lists () that might already be populated. It employs a context structure to pass the target relation ID and current sublevel information to the walker function.

## Parameters / Member Variables
- : PlannerInfo structure containing the query planning state and parse tree
- : The relation variable number (RTE index) to search for dependencies on

## Dependencies
- Functions called/Symbols referenced:
  - find_dependent_phvs_context (context structure)
  - [bms_make_singleton](../b/bms_make_singleton.md) (creates a singleton bitmap set)
  - query_tree_walker (traverses the query parse tree)
  - [find_dependent_phvs_walker](find_dependent_phvs_walker.md) (walker function for PHV detection)
  - expression_tree_walker (traverses expression trees)
- Called from (representative examples):
  - [remove_useless_results_recurse](../r/remove_useless_results_recurse.md) (in prepjointree.c:3701)

## Notes and Other Information
- This function is static and only used within prepjointree.c
- Returns early if no PlaceHolderVars exist in the query (performance optimization)
- The function checks both the main parse tree and append relation lists to ensure complete coverage
- Works in conjunction with find_dependent_phvs_walker which performs the actual PHV dependency checking
- Part of the query optimization phase where unused result relations are being identified and removed