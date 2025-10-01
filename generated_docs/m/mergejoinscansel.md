# mergejoinscansel

## Location
[src/backend/utils/adt/selfuncs.c:2956-3260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2956-L3260)

## Overview
Estimates scan selectivity for merge joins by calculating how much of each input stream will be read before the join terminates, which is critical for accurate cost estimation of indexed merge joins.

## Definition

```c
struct the merge clause */
	if (!is_opclause(clause))
		return;
```
## Detailed Description
The function analyzes a merge join clause to estimate scanning behavior of both input streams. Since merge joins stop as soon as either input stream is exhausted, understanding the data ranges of both inputs allows PostgreSQL to estimate how much data will actually be scanned. This is particularly important for index scans where the cost can vary dramatically based on how much of the index needs to be read.

The function works by:
1. Extracting variable statistics for both sides of the join clause
2. Looking up appropriate comparison operators based on the sort strategy (ascending/descending)
3. Getting data range estimates for both input variables
4. Computing selectivity estimates for where scanning starts and ends
5. Adjusting estimates to account for null values when nulls-first ordering is used

The estimates help determine both the total cost (how much data to scan) and startup cost (how long before the first matching tuple pair is found).

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and statistics
- : The merge join clause being analyzed (must be mergejoinable)
- : Operator family ID that defines the sort ordering
- : Sort strategy (BTLessStrategyNumber or BTGreaterStrategyNumber)
- : Whether nulls come first in the sort order
- : Output - fraction of left input scanned before first join pair (0-1)
- : Output - fraction of left input scanned when join terminates (0-1)
- : Output - fraction of right input scanned before first join pair (0-1)
- : Output - fraction of right input scanned when join terminates (0-1)

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md): Validates that clause is an operator expression
  - [get_leftop](../g/get_leftop.md)/get_rightop: Extract operands from the clause
  - [examine_variable](../e/examine_variable.md): Gather statistics for join variables
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md): Get operator properties from catalogs
  - [get_opfamily_member](../g/get_opfamily_member.md): Look up specific operators in the operator family
  - [get_variable_range](../g/get_variable_range.md): Extract min/max values from variable statistics
  - [scalarineqsel](../s/scalarineqsel.md): Estimate selectivity of inequality conditions
  - ReleaseVariableStats: Clean up variable statistics
- Called from (representative examples):
  - [cached_scansel](../c/cached_scansel.md): Uses merge join scan selectivity in cost calculations

## Notes and Other Information
- Sets default values (0.0 for start fractions, 1.0 for end fractions) if statistics are unavailable
- Only one of the "end" fractions can be less than 1.0 in practice - the function chooses the smaller estimate
- Only one of the "start" fractions can be greater than 0.0 in practice - the function chooses the larger estimate
- Handles both ascending (BTLessStrategyNumber) and descending (BTGreaterStrategyNumber) sort orders
- Accounts for cross-type comparisons where left and right operand types differ
- Adjusts estimates when nulls-first ordering is used by adding null fraction to start/end selectivities
- Falls back gracefully when operator family information is incomplete or statistics are unavailable

## Simplified Source

```c
void mergejoinscansel(PlannerInfo *root, Node *clause,
                      Oid opfamily, int strategy, bool nulls_first,
                      Selectivity *leftstart, Selectivity *leftend,
                      Selectivity *rightstart, Selectivity *rightend) {
    Node *left, *right;
    VariableStatData leftvar, rightvar;
    Oid opno, collation;
    Oid ltop, leop, revltop, revleop; // Comparison operators
    bool isgt;
    Datum leftmin, leftmax, rightmin, rightmax;
    double selec;

    // Set conservative defaults
    *leftstart = *rightstart = 0.0;
    *leftend = *rightend = 1.0;

    // Extract clause components
    if (!is_opclause(clause))
        return;
    opno = ((OpExpr *) clause)->opno;
    collation = ((OpExpr *) clause)->inputcollid;
    left = get_leftop((Expr *) clause);
    right = get_rightop((Expr *) clause);
    if (!right)
        return;

    // Gather variable statistics
    examine_variable(root, left, 0, &leftvar);
    examine_variable(root, right, 0, &rightvar);

    // Look up appropriate comparison operators based on sort strategy
    isgt = (strategy == BTGreaterStrategyNumber);
    // ... operator lookup logic based on ascending/descending and data types ...

    // Get variable ranges from statistics
    if (!get_variable_range(root, &leftvar, lstatop, collation, &leftmin, &leftmax) ||
        !get_variable_range(root, &rightvar, rstatop, collation, &rightmin, &rightmax))
        goto fail;

    // Calculate end fractions: how much to scan before join terminates
    selec = scalarineqsel(root, leop, isgt, true, collation, &leftvar,
                          rightmax, op_righttype);
    if (selec != DEFAULT_INEQ_SEL)
        *leftend = selec;

    selec = scalarineqsel(root, revleop, isgt, true, collation, &rightvar,
                          leftmax, op_lefttype);
    if (selec != DEFAULT_INEQ_SEL)
        *rightend = selec;

    // Only one end fraction can be < 1.0 - choose the smaller
    if (*leftend > *rightend)
        *leftend = 1.0;
    else if (*leftend < *rightend)
        *rightend = 1.0;
    else
        *leftend = *rightend = 1.0;

    // Calculate start fractions: how much to scan before first match
    selec = scalarineqsel(root, ltop, isgt, false, collation, &leftvar,
                          rightmin, op_righttype);
    if (selec != DEFAULT_INEQ_SEL)
        *leftstart = selec;

    selec = scalarineqsel(root, revltop, isgt, false, collation, &rightvar,
                          leftmin, op_lefttype);
    if (selec != DEFAULT_INEQ_SEL)
        *rightstart = selec;

    // Only one start fraction can be > 0.0 - choose the larger
    if (*leftstart < *rightstart)
        *leftstart = 0.0;
    else if (*leftstart > *rightstart)
        *rightstart = 0.0;
    else
        *leftstart = *rightstart = 0.0;

    // Adjust for nulls-first ordering if needed
    if (nulls_first) {
        // Add null fractions to start/end estimates and clamp to [0,1]
        // ... null fraction adjustment logic ...
    }

    // Sanity check: start must be < end
    if (*leftstart >= *leftend) {
        *leftstart = 0.0;
        *leftend = 1.0;
    }
    if (*rightstart >= *rightend) {
        *rightstart = 0.0;
        *rightend = 1.0;
    }

fail:
    ReleaseVariableStats(leftvar);
    ReleaseVariableStats(rightvar);
}
```