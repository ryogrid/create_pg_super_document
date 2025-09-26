# make_unique_from_pathkeys

## Location
[src/backend/optimizer/plan/createplan.c:6749-6854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6749-L6854)

## Overview
Creates a Unique plan node from pathkeys to eliminate duplicate rows based on pathkey specifications, handling both volatile and non-volatile equivalence classes.

## Definition
```c
static Unique *make_unique_from_pathkeys(Plan *lefttree, List *pathkeys, int numCols)
```

## Detailed Description
This static function constructs a Unique plan node using pathkeys to determine which columns should be considered for uniqueness filtering. Unlike `make_unique_from_sortclauses`, this function works with the optimizer's internal pathkey representation, which provides more abstract and flexible column specifications through equivalence classes. The function handles two different cases: volatile equivalence classes (which must match specific targetlist entries from ORDER BY clauses) and non-volatile equivalence classes (which can use any expression from the equivalence class found in the target list). It converts pathkey information into the array-based format required by the executor.

## Parameters / Member Variables
- `lefttree`: Left child plan node providing sorted input tuples
- `pathkeys`: List of PathKey structures identifying sort/unique columns
- `numCols`: Maximum number of columns to consider for uniqueness (may be less than total pathkeys)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create Unique node)
  - [palloc](../p/palloc.md) (to allocate arrays for column information)
  - [get_sortgroupref_tle](../g/get_sortgroupref_tle.md) (to find target entry by sort group reference for volatile ECs)
  - [find_ec_member_matching_expr](../f/find_ec_member_matching_expr.md) (to match expressions in equivalence classes)
  - [get_opfamily_member](../g/get_opfamily_member.md) (to find equality operator for pathkey's operator family)
  - elog (for error reporting)
- Types referenced:
  - [Unique](../U/Unique.md) (the uniqueness filtering plan node structure)
  - [PathKey](../P/PathKey.md) (structure representing sort/group ordering)
  - [EquivalenceClass](../E/EquivalenceClass.md) (class of equivalent expressions for ordering)
  - [EquivalenceMember](../E/EquivalenceMember.md) (member expression within an equivalence class)
  - [TargetEntry](../T/TargetEntry.md) (structure representing output columns)
- Called from (representative examples):
  - [create_upper_unique_plan](../c/create_upper_unique_plan.md)

## Notes and Other Information
- This is a static function, only accessible within the createplan.c file
- Handles volatile equivalence classes specially - they must match exact targetlist entries from ORDER BY
- For non-volatile equivalence classes, uses the first matching targetlist item found
- Uses BTEqualStrategyNumber to find appropriate equality operators from pathkey operator families  
- The numCols parameter allows processing only a subset of the provided pathkeys
- Contains a TODO comment suggesting potential code unification with `prepare_sort_from_pathkeys`
- More flexible than `make_unique_from_sortclauses` as it works with optimizer's abstract pathkey representation
- Includes comprehensive error checking with descriptive error messages for missing operators
- The right child plan node is always set to NULL as uniqueness filtering is a unary operation
- Commonly used in upper-level query plan nodes where pathkey-based uniqueness is needed