# Result

## Location
[src/include/nodes/plannodes.h:196-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L196-L200)

## Overview
Result is a plan node that either evaluates a variable-free targetlist (when there's no outer plan) or applies projection to tuples from an outer plan, with optional one-time qualification testing.

## Definition


## Detailed Description
The Result node serves two primary purposes in PostgreSQL's execution engine. When used without an outer plan (lefttree is NULL), it evaluates expressions that don't require input tuples, such as SELECT 1+1 or SELECT current_timestamp. When used with an outer plan, it acts as a projection node that applies the targetlist to incoming tuples.

The resconstantqual field contains qualification conditions that need to be evaluated only once, independent of any variables from the outer plan. This is useful for optimizing queries where certain conditions can be checked upfront rather than for every tuple.

Result nodes are commonly used in various optimization scenarios, including gating plans (where a condition determines whether to execute a subplan), implementing LIMIT clauses, and handling constant expressions in SELECT lists.

## Parameters / Member Variables
- : Base Plan structure containing common fields like costs, targetlist, and tree structure
- : Optional one-time qualification test that doesn't depend on outer plan variables

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - [Node](../N/Node.md)

- Called from (representative examples):
  - [make_result](../m/make_result.md) (optimizer/plan/createplan.c:6993)
  - [create_gating_plan](../c/create_gating_plan.md) (optimizer/plan/createplan.c:1038,1040)
  - [create_group_result_plan](../c/create_group_result_plan.md) (optimizer/plan/createplan.c:1590)
  - [create_resultscan_plan](../c/create_resultscan_plan.md) (optimizer/plan/createplan.c:4028)
  - [ExecInitResult](../E/ExecInitResult.md) (executor/nodeResult.c:180)
  - [make_limit](../m/make_limit.md) (optimizer/plan/createplan.c:6988)

## Notes and Other Information
- [Result](Result.md) nodes are versatile and appear in many different query execution scenarios
- When resconstantqual evaluates to false, the Result node can terminate execution early
- Commonly used as a wrapper for constant expressions and scalar subqueries
- Can serve as a projection layer when complex expressions need to be evaluated on incoming tuples
- The simplicity of the Result node makes it an efficient choice for operations that don't require complex data access patterns
- Often appears at the top level of plans for queries that return constant values or perform simple calculations