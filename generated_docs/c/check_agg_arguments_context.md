# check_agg_arguments_context

## Location
src/backend/parser/parse_agg.c: 41 - 53

## Overview
A context structure used during parsing to track the minimum variable and aggregate levels found while walking through aggregate function arguments to detect nested aggregation violations.

## Definition
```c
typedef struct
{
    ParseState *pstate;
    int         min_varlevel;
    int         min_agglevel;
    int         sublevels_up;
} check_agg_arguments_context;
```

## Detailed Description
The `check_agg_arguments_context` structure serves as a walking context for the PostgreSQL parser when analyzing aggregate function arguments to detect illegal nested aggregation. It maintains state information during a recursive tree walk to track the minimum nesting levels of variables and aggregates encountered within aggregate function arguments.

The primary purpose is to enforce SQL standard rules that prohibit nested aggregates at the same semantic level. When PostgreSQL processes an aggregate function, it needs to determine if there are any nested aggregates within the arguments that would violate SQL semantics. This structure accumulates the minimum levels found during the traversal, allowing the parser to make appropriate decisions about whether to allow the aggregation or throw an error.

The structure is used in conjunction with `check_agg_arguments_walker()` which recursively walks through expression trees, updating the context as it encounters Var nodes (variables) and Aggref nodes (aggregate references). The collected information is then used by `check_agg_arguments()` to determine the semantic level of the outer aggregate and detect conflicts.

## Parameters / Member Variables
- `pstate`: Pointer to the current ParseState, providing access to parsing context and state information
- `min_varlevel`: Minimum variable level found so far (-1 indicates no variables found yet), tracks the lowest nesting level of variables encountered
- `min_agglevel`: Minimum aggregate level found so far (-1 indicates no aggregates found yet), tracks the lowest nesting level of nested aggregates
- `sublevels_up`: Current nesting depth adjustment for converting between different frames of reference during subquery processing

## Dependencies
- Functions called/Symbols referenced:
  - [ParseState](../P/ParseState.md) (structure type)
- Called from (representative examples):
  - [check_agg_arguments](check_agg_arguments.md) (src/backend/parser/parse_agg.c:642)
  - [check_agg_arguments_walker](check_agg_arguments_walker.md) (src/backend/parser/parse_agg.c:718)

## Notes and Other Information
- This structure is closely related to `check_ungrouped_columns_context` which serves a similar purpose for GROUP BY validation
- The negative values (-1) for `min_varlevel` and `min_agglevel` serve as sentinel values indicating that no variables or aggregates have been found yet during the tree walk
- The `sublevels_up` field is crucial for handling subqueries correctly, as it adjusts level calculations to maintain the proper frame of reference when dealing with nested query structures
- This context is part of PostgreSQL's comprehensive aggregate validation system that ensures SQL standard compliance for aggregate expressions