# SubLinkType

## Location
src/include/nodes/primnodes.h: 1005 - 1007

## Overview
SubLinkType is an enumeration that defines the different types of subselect expressions supported in PostgreSQL, specifying how subqueries are combined with their surrounding expressions.

## Definition


## Detailed Description
SubLinkType categorizes the various forms of subselect expressions that can appear in SQL queries. Each type represents a different semantic behavior for how the subquery interacts with the outer query:

- **EXISTS_SUBLINK**: Represents EXISTS(SELECT ...) expressions that test for the existence of rows
- **ALL_SUBLINK**: Represents (lefthand) op ALL (SELECT ...) expressions where all rows must satisfy the condition
- **ANY_SUBLINK**: Represents (lefthand) op ANY (SELECT ...) expressions where at least one row must satisfy the condition  
- **ROWCOMPARE_SUBLINK**: Represents (lefthand) op (SELECT ...) for row-wise comparisons with multiple columns
- **EXPR_SUBLINK**: Represents scalar subqueries (SELECT with single targetlist item) that return a single value
- **MULTIEXPR_SUBLINK**: Represents subqueries with multiple targetlist items for multiple-assignment scenarios
- **ARRAY_SUBLINK**: Represents ARRAY(SELECT ...) expressions that construct arrays from subquery results
- **CTE_SUBLINK**: Represents WITH query subplans (used only in SubPlans, not actual SubLink nodes)

The enumeration handles different cardinality requirements - some types expect at most one row (EXPR, MULTIEXPR, ROWCOMPARE), while others can process multiple rows (ALL, ANY, ARRAY).

## Parameters / Member Variables
- : Tests for row existence (returns boolean)
- : All rows must satisfy condition (combines results with AND semantics)
- : At least one row must satisfy condition (combines results with OR semantics)
- : Row-wise comparison (always multiple columns, at most one result row)
- : Scalar subquery (single value, at most one result row)
- : Multiple-assignment subquery (multiple values, at most one result row)
- : Array construction (single column, any number of rows)
- : Common Table Expression subplan (SubPlans only)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this enum)
- Called from (representative examples):
  - SubLink struct (uses SubLinkType as subLinkType field)
  - SubPlan struct (uses SubLinkType as subLinkType field)
  - [ExecScanSubPlan](../E/ExecScanSubPlan.md) function
  - [ExecSetParamPlan](../E/ExecSetParamPlan.md) function
  - [make_subplan](../m/make_subplan.md) function
  - [build_subplan](../b/build_subplan.md) function

## Notes and Other Information
- SubLink nodes are not directly executable and must be replaced by SubPlan nodes during query planning
- ROWCOMPARE_SUBLINK always involves multiple columns; single-column comparisons use EXPR_SUBLINK instead
- ALL and ANY types require boolean-returning combining operators
- MULTIEXPR_SUBLINK uses subLinkId for identifying different multiple-assignment subqueries within UPDATE statements
- CTE_SUBLINK appears only in SubPlans generated for WITH subqueries, never in actual SubLink nodes
- The lefthand expressions for ALL, ANY, and ROWCOMPARE must match the length of the subselect's targetlist
- Critical for proper parsing, planning, and execution of complex subquery expressions in PostgreSQL