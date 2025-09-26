# build_aggregate_serialfn_expr

## Location
[src/backend/parser/parse_agg.c:2096-2118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L2096-L2118)

## Overview
Creates an expression tree for the serialization function of an aggregate, similar to build_aggregate_transfn_expr but specifically for serialization operations.

## Definition
```c
void build_aggregate_serialfn_expr(Oid serialfn_oid, Expr **serialfnexpr)
```

## Detailed Description
This function constructs an expression tree for aggregate serialization functions, which are used in parallel aggregation to serialize intermediate aggregate state values for transfer between worker processes and the leader process.

The function creates a FuncExpr node representing a call to the serialization function. Serialization functions in PostgreSQL have a standardized signature:
- Input: INTERNAL (representing the aggregate's internal state)
- Output: BYTEA (serialized representation of the state)

The expression tree serves the same purpose as those created by build_aggregate_transfn_expr: providing type information for polymorphic function resolution, though the expressions are never actually executed.

Unlike transition functions which have variable argument lists depending on the aggregate, serialization functions always have the same signature, making this function much simpler than its transition function counterpart.

## Parameters / Member Variables
- `serialfn_oid`: OID of the serialization function to create an expression for
- `serialfnexpr`: Output pointer for the constructed serialization function expression

## Dependencies
- Functions called/Symbols referenced:
  - `[FuncExpr](../F/FuncExpr.md)` (struct type for function call expressions)
  - [make_agg_arg](../m/make_agg_arg.md)() (creates argument nodes for aggregates)
  - `[makeFuncExpr](../m/makeFuncExpr.md)()` (creates function call expression nodes)
  - `COERCE_EXPLICIT_CALL` (constant for function coercion context)
  - `INTERNALOID`, `BYTEAOID` (type constants)
  - `InvalidOid` (constant for invalid OID values)
  - `list_make1()` (creates single-element list)
- Called from (representative examples):
  - [build_pertrans_for_aggref](build_pertrans_for_aggref.md) (in nodeAgg.c)

## Notes and Other Information
- Serialization functions always have the signature: serialfn(internal) → bytea
- Used exclusively in the context of parallel aggregation where intermediate state values need to be transferred between processes
- The created expression is used for type resolution only and never executed
- Much simpler than build_aggregate_transfn_expr due to the standardized serialization function signature
- Part of the parallel aggregation infrastructure in PostgreSQL