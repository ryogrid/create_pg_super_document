# build_aggregate_transfn_expr

## Location
src/backend/parser/parse_agg.c: 2035 - 2095

## Overview
Constructs expression trees for transition and inverse transition functions of aggregate operations, enabling polymorphic functions to determine their required datatypes.

## Definition
```c
void build_aggregate_transfn_expr(Oid *agg_input_types,
                                 int agg_num_inputs,
                                 int agg_num_direct_inputs,
                                 bool agg_variadic,
                                 Oid agg_state_type,
                                 Oid agg_input_collation,
                                 Oid transfn_oid,
                                 Oid invtransfn_oid,
                                 Expr **transfnexpr,
                                 Expr **invtransfnexpr)
```

## Detailed Description
This function creates expression trees for aggregate transition functions, which is essential for polymorphic function resolution within aggregates. Without these expression trees, polymorphic functions would not know what datatypes they should operate on.

The function builds FuncExpr nodes that represent calls to the transition function (and optionally the inverse transition function). The expression trees are never actually executed but serve as type information carriers.

Key functionality includes:
1. Building argument lists starting with the aggregate state type
2. Adding aggregated arguments (skipping direct arguments for ordered-set aggregates)
3. Creating FuncExpr nodes with proper type and collation information
4. Handling variadic functions appropriately
5. Optionally building inverse transition function expressions with the same argument structure

For ordered-set aggregates, the function correctly handles the distinction between direct arguments and aggregated arguments, only including the latter in the transition function calls.

## Parameters / Member Variables
- `agg_input_types`: Array of resolved input types for the aggregate (no polymorphic types)
- `agg_num_inputs`: Total number of input arguments
- `agg_num_direct_inputs`: Number of direct arguments (for ordered-set aggregates)
- `agg_variadic`: Whether the aggregate function is variadic
- `agg_state_type`: The resolved state datatype of the aggregate
- `agg_input_collation`: Collation to use for the aggregate inputs
- `transfn_oid`: OID of the transition function (or combine function)
- `invtransfn_oid`: OID of the inverse transition function (may be InvalidOid)
- `transfnexpr`: Output pointer for the constructed transition function expression
- `invtransfnexpr`: Output pointer for the constructed inverse transition function expression (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `FuncExpr` (struct type for function call expressions)
  - `[make_agg_arg](../m/make_agg_arg.md)()` (creates argument nodes for aggregates)
  - `makeFuncExpr()` (creates function call expression nodes)
  - `COERCE_EXPLICIT_CALL` (constant for function coercion context)
  - `list_make1()`, `lappend()` (list manipulation)
  - `OidIsValid()` (OID validation)
- Called from (representative examples):
  - `[build_pertrans_for_aggref](build_pertrans_for_aggref.md)` (in nodeAgg.c)
  - `initialize_peragg` (in nodeWindowAgg.c)

## Notes and Other Information
- The constructed expressions are used for type resolution only and are never executed
- Can be used for both regular transition functions and combine functions in parallel aggregation
- Inverse transition functions are optional - when invtransfn_oid is InvalidOid, no inverse expression is built
- For combine functions, inverse transition functions are not applicable (no inverse combine function exists)
- Properly handles variadic aggregates by setting the funcvariadic flag on created FuncExpr nodes