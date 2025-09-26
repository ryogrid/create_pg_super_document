# build_aggregate_finalfn_expr

## Location
[src/backend/parser/parse_agg.c:2143-2182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L2143-L2182)

## Overview
Constructs an expression tree for the final function of an aggregate, creating the necessary function call structure to produce the final aggregate result from the accumulated state.

## Definition
```c
void build_aggregate_finalfn_expr(Oid *agg_input_types,
                                  int num_finalfn_inputs,
                                  Oid agg_state_type,
                                  Oid agg_result_type,
                                  Oid agg_input_collation,
                                  Oid finalfn_oid,
                                  Expr **finalfnexpr)
```

## Detailed Description
This function builds an expression tree for aggregate final functions, which are responsible for converting the accumulated aggregate state into the final result value. The final function always takes the aggregate state as its first argument, and may optionally take additional arguments that match the aggregate input types. This is part of PostgreSQL aggregate processing infrastructure and works similarly to `build_aggregate_transfn_expr` but for the finalization phase.

The function handles both simple final functions (that only take the state) and more complex ones that require access to the original aggregate input values during finalization.

## Parameters / Member Variables
- `agg_input_types`: Array of OIDs representing the input types of the aggregate
- `num_finalfn_inputs`: Number of inputs the final function expects (including state)
- `agg_state_type`: OID of the aggregate internal state type
- `agg_result_type`: OID of the aggregate final result type
- `agg_input_collation`: Collation to use for the aggregate inputs
- `finalfn_oid`: OID of the final function to be called
- `finalfnexpr`: Output parameter that receives the constructed expression tree

## Dependencies
- Functions called/Symbols referenced:
  - list_make1
  - [make_agg_arg](../m/make_agg_arg.md)
  - [lappend](../l/lappend.md)
  - [makeFuncExpr](../m/makeFuncExpr.md)
  - COERCE_EXPLICIT_CALL
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [initialize_peragg](../i/initialize_peragg.md)

## Notes and Other Information
- The first argument is always the aggregate state, additional arguments match input types
- Uses COERCE_EXPLICIT_CALL for function call coercion
- Final functions are never treated as variadic
- Part of PostgreSQL aggregate execution infrastructure
- Located in src/backend/parser/parse_agg.c:2143-2182
- Handles both simple and complex final function signatures dynamically