# clause_selectivity_ext

## Location
[src/backend/optimizer/path/clausesel.c:684-973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/clausesel.c#L684-L973)

## Overview
Extended version of clause_selectivity that provides fine-grained control over extended statistics usage and implements the core logic for computing selectivity of general boolean expression clauses.

## Definition
```c
Selectivity
clause_selectivity_ext(PlannerInfo *root,
                       Node *clause,
                       int varRelid,
                       JoinType jointype,
                       SpecialJoinInfo *sjinfo,
                       bool use_extended_stats)
```

## Detailed Description
This function serves as the comprehensive selectivity estimation engine for PostgreSQL's query optimizer. It handles a wide variety of clause types including variables, constants, parameters, logical operations (NOT, AND, OR), operator expressions, function calls, scalar array operations, row comparisons, null tests, boolean tests, and type coercion expressions.

Key features include:
1. **Caching mechanism**: For RestrictInfo clauses, results are cached in norm_selec (JOIN_INNER) or outer_selec (other join types) fields to avoid redundant calculations
2. **Extended statistics control**: The use_extended_stats parameter allows selective enabling/disabling of extended statistics for fine-tuned estimation
3. **Pseudoconstant handling**: Pseudoconstant clauses return 1.0 selectivity (except FALSE constants which return 0.0)
4. **Recursive processing**: Handles complex nested expressions by recursively calling itself for subexpressions
5. **Join vs. restriction classification**: Uses treat_as_join_clause to determine appropriate selectivity estimation method

The function implements sophisticated logic for different expression types:
- **Variables**: Uses boolvarsel for boolean variables
- **Constants**: Returns exact selectivity (0.0 for FALSE/NULL, 1.0 for TRUE)
- **Logical operations**: Implements proper AND/OR logic and NOT inversion
- **Operator clauses**: Delegates to join_selectivity or restriction_selectivity based on clause classification
- **Function calls**: Uses function-specific selectivity estimation
- **Special constructs**: Handles array operations, row comparisons, null tests, and boolean tests

## Parameters / Member Variables
- `root`: PlannerInfo structure containing optimizer state and statistics
- `clause`: Node representing the boolean expression (RestrictInfo or plain expression)
- `varRelid`: Relation ID for restriction mode (0 for join mode)
- `jointype`: Type of join operation affecting selectivity calculation
- `sjinfo`: SpecialJoinInfo providing join context information
- `use_extended_stats`: Flag controlling whether to use extended statistics

## Dependencies
- Functions called/Symbols referenced:
  - [treat_as_join_clause](../t/treat_as_join_clause.md) (for clause classification)
  - [clauselist_selectivity_ext](clauselist_selectivity_ext.md) (for AND clauses)
  - [clauselist_selectivity_or](clauselist_selectivity_or.md) (for OR clauses) 
  - [join_selectivity](../j/join_selectivity.md) (for join clauses)
  - [restriction_selectivity](../r/restriction_selectivity.md) (for restriction clauses)
  - [function_selectivity](../f/function_selectivity.md) (for function expressions)
  - [boolvarsel](../b/boolvarsel.md) (for boolean variables)
  - [scalararraysel](../s/scalararraysel.md), rowcomparesel, nulltestsel, booltestsel (for specific node types)
  - [estimate_expression_value](../e/estimate_expression_value.md) (for parameter evaluation)
- Called from (representative examples):
  - [clause_selectivity](clause_selectivity.md) (standard interface)
  - [clauselist_selectivity_ext](clauselist_selectivity_ext.md) (for recursive AND processing)
  - [clauselist_selectivity_or](clauselist_selectivity_or.md) (for recursive OR processing)
  - [statext_mcv_clauselist_selectivity](../s/statext_mcv_clauselist_selectivity.md) (extended statistics)

## Notes and Other Information
- Default selectivity is 0.5 for unhandled clause types
- Implements sophisticated caching strategy based on varRelid and join context
- Supports debugging through SELECTIVITY_DEBUG compilation flag
- Handles type coercion transparently (RelabelType, CoerceToDomain)
- Uses different cache fields for INNER vs. outer joins to handle examination with different join types
- Contains extensive comments explaining caching conditions and join type variations
- The function is central to PostgreSQL's cost-based optimization and affects query plan selection significantly