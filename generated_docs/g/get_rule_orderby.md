# get_rule_orderby

## Location
[src/backend/utils/adt/ruleutils.c:6448-6505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6448-L6505)

## Overview
Formats and outputs an ORDER BY clause as a string representation for SQL rule deparsing, including sort direction, null ordering, and custom operators.

## Definition
```c
static void get_rule_orderby(List *orderList, List *targetList, 
                             bool force_colno, deparse_context *context)
```

## Detailed Description
This function converts internal ORDER BY representations into their textual SQL form during query deparsing. It processes each sort specification in the order list, determining:

- The column expression or reference to sort by
- Sort direction (ASC/DESC) based on comparison operators
- NULL ordering behavior (NULLS FIRST/NULLS LAST)
- Custom sort operators when non-standard operators are used

The function intelligently handles default behaviors (ASC is implicit, DESC defaults to NULLS FIRST) and only outputs explicit clauses when they differ from defaults. For custom operators, it uses the USING clause syntax and always specifies null ordering explicitly.

## Parameters / Member Variables
- `orderList`: List of SortGroupClause structures defining the sort specifications
- `targetList`: Target list containing the expressions that can be referenced for sorting
- `force_colno`: Boolean flag to force column number output instead of expression text
- `context`: Deparse context containing output buffer and formatting information

## Dependencies
- Functions called/Symbols referenced:
  - [get_rule_sortgroupclause](get_rule_sortgroupclause.md) (to resolve sort column expressions)
  - [exprType](../e/exprType.md) (to determine the data type of sort expressions)
  - [lookup_type_cache](../l/lookup_type_cache.md) (to get default less-than and greater-than operators)
  - [generate_operator_name](generate_operator_name.md) (for custom operator names)
- Called from (representative examples):
  - [get_select_query_def](get_select_query_def.md) (for main query ORDER BY clauses)
  - [get_rule_windowspec](get_rule_windowspec.md) (for window function ORDER BY specifications)
  - [get_agg_expr_helper](get_agg_expr_helper.md) (for aggregate function ordering)

## Notes and Other Information
- Static function accessible only within ruleutils.c
- Handles PostgreSQL's three-valued logic for NULL ordering
- Optimizes output by omitting default ASC and default NULLS positioning
- Located at src/backend/utils/adt/ruleutils.c:6448-6505
- Essential component of query reconstruction for views, rules, and function definitions