# booltestsel

## Location
src/backend/utils/adt/selfuncs.c: 1541 - 1698

## Overview
Computes the selectivity of BooleanTest nodes, handling SQL Boolean test operations like IS TRUE, IS FALSE, IS UNKNOWN and their negated forms.

## Definition

```c
Selectivity
booltestsel(PlannerInfo *root, BoolTestType booltesttype, Node *arg,
			int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
```
## Detailed Description
The  function estimates selectivity for Boolean test expressions in SQL queries, such as , , , etc. It implements sophisticated logic to handle the three-valued Boolean logic of SQL (TRUE, FALSE, NULL/UNKNOWN).

The function operates in three tiers of sophistication:

1. **Full Statistics Available**: When both most-common-values (MCV) and null fraction statistics are available, it calculates precise frequencies for TRUE, FALSE, and NULL values, then applies the appropriate Boolean test logic.

2. **Partial Statistics**: When only null fraction data is available, it uses that for IS [NOT] UNKNOWN tests and assumes a 50-50 split between TRUE and FALSE for non-NULL values.

3. **No Statistics**: Falls back to using  on the underlying expression with default selectivity constants for UNKNOWN tests.

The function handles all six Boolean test types: IS TRUE, IS NOT TRUE, IS FALSE, IS NOT FALSE, IS UNKNOWN, and IS NOT UNKNOWN, each with specific selectivity calculation logic that respects SQL's three-valued logic.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Type of Boolean test (IS_TRUE, IS_FALSE, IS_UNKNOWN, IS_NOT_TRUE, IS_NOT_FALSE, IS_NOT_UNKNOWN)
- : Node representing the expression being tested
- : Relation ID to restrict analysis to (0 if no restriction)
- : Type of join operation context
- : Special join information for outer joins

## Dependencies
- Functions called/Symbols referenced:
  - examine_variable
  - get_attstatsslot
  - free_attstatsslot
  - clause_selectivity
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - clause_selectivity_ext
  - GenericCosts

## Notes and Other Information
- Handles SQL's three-valued Boolean logic (TRUE, FALSE, NULL/UNKNOWN) correctly
- Uses sophisticated statistical analysis when MCV (most-common-values) data is available
- Falls back gracefully through multiple levels of statistical data availability
- Ensures result is within valid probability range using CLAMP_PROBABILITY
- Critical for accurate selectivity estimation of Boolean test operations in query optimization
- Supports all six Boolean test operations defined in SQL standard