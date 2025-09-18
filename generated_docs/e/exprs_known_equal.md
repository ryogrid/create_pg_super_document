# exprs_known_equal

## Location
[src/backend/optimizer/path/equivclass.c:2449-2499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L2449-L2499)

## Overview
Detects whether two expressions are known to be equal based on equivalence relationships established during query optimization, primarily used for selectivity estimation.

## Definition
```c
bool exprs_known_equal(PlannerInfo *root, Node *item1, Node *item2)
```

## Detailed Description
This function determines if two expressions are considered equal according to the equivalence classes maintained by PostgreSQL's query optimizer. It searches through all equivalence classes in the PlannerInfo structure to see if both expressions are members of the same equivalence class.

The function uses a "fuzzy" notion of equality based on operator family definitions rather than strict structural equality. This is acceptable for its primary use case of selectivity estimation, where approximate equality relationships are sufficient for generating reasonable cost estimates.

The function specifically skips volatile equivalence classes (those containing volatile functions) and child equivalence members (used for inheritance hierarchies), focusing only on stable, parent-level equivalences. It performs an early exit optimization, returning true as soon as both expressions are found in the same equivalence class.

Note that the function does not check for structural equality (equal(item1, item2)) - the caller is expected to handle that case if identical expressions might be passed.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the equivalence classes for the current query
- `item1`: First expression node to compare
- `item2`: Second expression node to compare

## Dependencies
- Functions called/Symbols referenced:
  - EquivalenceClass (struct type)
  - [EquivalenceMember](../E/EquivalenceMember.md) (struct type)
  - [equal](equal.md) (expression comparison function)
  - lfirst (list iteration macro)
- Called from (representative examples):
  - [add_unique_group_var](../a/add_unique_group_var.md)
  - Referenced in paths.h header

## Notes and Other Information
- Returns false if no equivalence relationship is found between the expressions
- Skips volatile equivalence classes to avoid incorrect assumptions about equality
- Ignores child equivalence members to focus on parent-level relationships
- Used primarily for selectivity estimation rather than correctness-critical equality checking
- The caller must handle the case where the two expressions might be structurally identical
- Early exit optimization stops searching as soon as both expressions are found in the same equivalence class