# is_simple_values

## Location
[src/backend/optimizer/prep/prepjointree.c:1895-1953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L1895-1953)

## Overview
Determines whether a VALUES RTE (Range Table Entry) is simple enough to be pulled up and optimized by replacing it with a more efficient RESULT RTE.

## Definition

```c
static bool
is_simple_values(PlannerInfo *root, RangeTblEntry *rte)
```
## Detailed Description
This function analyzes a VALUES clause to determine if it qualifies for optimization through the  function. It applies several strict criteria to ensure that the VALUES clause can be safely replaced with a RESULT RTE without changing query semantics or introducing performance issues.

The function performs the following checks:

1. **Single Row Requirement**: The VALUES clause must contain exactly one row (one values_lists entry). Multiple rows would make it semantically incorrect to replace the VALUES RTE with a RESULT RTE, and there would be no unique set of expressions to substitute into the parent query.

2. **Set-Returning Functions**: Rejects VALUES clauses that contain set-returning functions, which could change the number of result rows and affect query semantics after pullup.

3. **Volatile Functions**: Prevents pullup of VALUES containing volatile functions to avoid multiple evaluations that could lead to different results than the original query.

4. **Single RTE Requirement**: Currently only allows pullup when the VALUES is the sole RTE in its query. This restriction greatly simplifies the pullup process and matches the typical parser output for simple VALUES clauses.

The restrictions mirror those applied to subquery pullup optimization, ensuring consistent behavior across different types of query optimizations.

## Parameters / Member Variables
- : PlannerInfo containing the overall query planning context and the parse tree being analyzed
- : The RangeTblEntry of type RTE_VALUES that is being evaluated for pullup eligibility

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - [expression_returns_set](../e/expression_returns_set.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - linitial
- Called from (representative examples):
  - [pull_up_subqueries_recurse](../p/pull_up_subqueries_recurse.md)

## Notes and Other Information
- The function is static, limiting its scope to the prepjointree.c compilation unit
- Returns true only if the VALUES clause meets all criteria for safe pullup optimization
- The single-row restriction is fundamental to the optimization - multi-row VALUES would require different optimization strategies
- LATERAL considerations are explicitly noted as not applying since VALUES cannot appear under outer joins in contexts where pullup would be attempted
- The single RTE restriction reflects current parser limitations and simplifies the pullup implementation significantly
- The function uses assertions to verify that the RTE is indeed of type RTE_VALUES, indicating it should only be called in appropriate contexts
- This function works in conjunction with  to provide a complete optimization path for simple constant value expressions

## Simplified Source

```c
static bool
is_simple_values(PlannerInfo *root, RangeTblEntry *rte)
{
    // Must have exactly one VALUES row
    if (list_length(rte->values_lists) != 1)
        return false;

    // Reject set-returning or volatile functions
    if (expression_returns_set((Node *) rte->values_lists) ||
        contain_volatile_functions((Node *) rte->values_lists))
        return false;

    // VALUES must be the only RTE in the query
    if (list_length(root->parse->rtable) != 1 ||
        rte != (RangeTblEntry *) linitial(root->parse->rtable))
        return false;

    return true;
}
```