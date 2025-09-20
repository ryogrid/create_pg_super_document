# add_unique_group_var

## Location
[src/backend/utils/adt/selfuncs.c:3300-3428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L3300-L3428)

## Overview
Maintains a list of unique group variables by adding new variables while avoiding duplicates and handling known-equal variables from different relations for group estimation purposes.

## Definition

```c
static List *
add_unique_group_var(PlannerInfo *root, List *varinfos,
					 Node *var, VariableStatData *vardata)
```
## Detailed Description
This static function manages a collection of GroupVarInfo structures, ensuring that each unique variable is represented only once while handling special cases for equivalent variables from different relations. The function is used during GROUP BY cardinality estimation to build a deduplicated list of variables that affect the number of groups.

Key behaviors include:
1. Extracting the number of distinct values for the variable using available statistics
2. Normalizing the variable by removing nulling relation markers to enable proper duplicate detection
3. Checking for exact duplicates and skipping them
4. Identifying known-equal variables from different relations and keeping only the one with better statistics (lower ndistinct value indicates better statistics)
5. Creating a new GroupVarInfo entry if the variable is truly unique

The function is essential for accurate GROUP BY cardinality estimation, as it ensures that equivalent variables don't artificially inflate the estimated number of groups.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and equivalence class information
- : Existing list of GroupVarInfo structures to extend
- : The variable node to potentially add to the list
- : VariableStatData containing statistics and relation information for the variable

## Dependencies
- Functions called/Symbols referenced:
  - get_variable_numdistinct: Extract distinct value count from variable statistics
  - [remove_nulling_relids](../r/remove_nulling_relids.md): Strip nulling relation markers for comparison
  - [equal](../e/equal.md): Test for exact node equality
  - [exprs_known_equal](../e/exprs_known_equal.md): Check if expressions are known to be equivalent
  - foreach_delete_current: Remove current element from list during iteration
  - GroupVarInfo: Structure to store variable information for group estimation
- Called from (representative examples):
  - [estimate_num_groups](../e/estimate_num_groups.md): Uses this function to build the variable list for cardinality estimation

## Notes and Other Information
- Returns the updated varinfos list, which may be the same as input if variable was duplicate
- Handles outer join nulling relation markers by stripping them before comparison
- Implements a "better statistics wins" policy when choosing between known-equal variables
- The isdefault flag tracks whether the ndistinct estimate is a default value or from actual statistics
- Critical for preventing double-counting of equivalent variables in GROUP BY cardinality estimation
- Only considers variables from different relations as potentially equivalent (same-relation variables are assumed distinct)
- Memory management uses palloc for new GroupVarInfo allocation and lappend for list extension