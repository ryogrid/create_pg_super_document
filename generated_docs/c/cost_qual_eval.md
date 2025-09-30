# cost_qual_eval

## Location
[src/backend/optimizer/path/costsize.c:4643-4668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L4643-L4668)

## Overview
Estimates the CPU costs of evaluating a WHERE clause, providing both startup and per-tuple evaluation costs.

## Definition

```c
void
cost_qual_eval(QualCost *cost, List *quals, PlannerInfo *root)
```
## Detailed Description
The  function computes cost estimates for evaluating boolean qualification expressions (WHERE clause conditions). It processes a list of qualification expressions and calculates both one-time startup costs and per-tuple evaluation costs. The function serves as the main entry point for qualification cost estimation throughout the PostgreSQL query planner.

The function accepts either an implicitly-ANDed list of boolean expressions or a list of RestrictInfo nodes (preferred for caching benefits). It initializes a cost evaluation context and walks through each qualification expression using  to accumulate the total costs. The implicit ANDing at the top level incurs no additional cost.

This function is fundamental to PostgreSQL's cost-based optimization, as qualification evaluation costs significantly impact plan selection decisions across all scan and join methods.

## Parameters / Member Variables
- : Output parameter receiving the calculated QualCost structure with startup and per_tuple components
- : List of qualification expressions (Node* or RestrictInfo*) to evaluate
- : PlannerInfo context for planning information (can be NULL, resulting in slightly worse estimates)

## Dependencies
- Functions called/Symbols referenced:
  - [cost_qual_eval_walker](cost_qual_eval_walker.md)
  - cost_qual_eval_context (struct)
  - [QualCost](../Q/QualCost.md) (struct)
- Called from (representative examples):
  - [cost_index](cost_index.md)
  - [cost_tidscan](cost_tidscan.md)
  - [final_cost_nestloop](../f/final_cost_nestloop.md)
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - [set_baserel_size_estimates](../s/set_baserel_size_estimates.md)

## Notes and Other Information
- Widely used throughout the cost estimation system for all plan node types that evaluate qualifications
- [RestrictInfo](../R/RestrictInfo.md) input is preferred over raw expressions because it enables cost caching
- Root parameter can be NULL in some code paths, which reduces estimation accuracy
- Does not charge cost for implicit ANDing at the top level of qualification lists
- Results are accumulated in a cost_qual_eval_context structure before final assignment
- Critical for selectivity and cost estimation in scan operations, joins, and aggregations

## Simplified Source

```c
void
cost_qual_eval(QualCost *cost, List *quals, PlannerInfo *root)
{
    cost_qual_eval_context context;
    ListCell *l;

    // Initialize cost evaluation context
    context.root = root;
    context.total.startup = 0;
    context.total.per_tuple = 0;

    // Walk through each qualification expression and accumulate costs
    foreach(l, quals) {
        Node *qual = (Node *) lfirst(l);
        cost_qual_eval_walker(qual, &context);
    }

    // Return accumulated costs
    *cost = context.total;
}
```