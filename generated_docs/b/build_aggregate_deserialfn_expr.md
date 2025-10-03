# build_aggregate_deserialfn_expr

## Location
[src/backend/parser/parse_agg.c:2119-2142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L2119-L2142)

## Overview
Constructs an expression tree for the deserialization function of an aggregate, creating the necessary function call structure for deserializing aggregate state data.

## Definition

```c
void
build_aggregate_deserialfn_expr(Oid deserialfn_oid,
								Expr **deserialfnexpr)
```
## Detailed Description
This function builds an expression tree specifically for aggregate deserialization functions. It creates a FuncExpr node that represents a call to the deserialization function, which is used in parallel aggregation to deserialize the state that was previously serialized by another worker process. The deserialization function always follows a fixed signature: it takes BYTEA (serialized data) and INTERNAL (context) parameters and returns INTERNAL (deserialized state).

The function is part of PostgreSQL's aggregate expression building infrastructure, similar to  but specialized for the deserialization phase of parallel aggregate processing.

## Parameters / Member Variables
- `deserialfn_oid`: The OID of the deserialization function to be called
- `**deserialfnexpr`: Output parameter that receives the constructed expression tree (FuncExpr cast to Expr*)
## Dependencies
- Functions called/Symbols referenced:
  - list_make2
  - [make_agg_arg](../m/make_agg_arg.md)
  - [makeFuncExpr](../m/makeFuncExpr.md)
  - [FuncExpr](../F/FuncExpr.md) (struct type)
  - COERCE_EXPLICIT_CALL (constant)
- Called from (representative examples):
  - [build_pertrans_for_aggref](build_pertrans_for_aggref.md) (in nodeAgg.c)

## Notes and Other Information
- The deserialization function always has a fixed signature: (BYTEA, INTERNAL) -> INTERNAL
- Uses COERCE_EXPLICIT_CALL for function call coercion type
- Part of PostgreSQL's parallel aggregation infrastructure
- Located in src/backend/parser/parse_agg.c:2119-2142
- The function creates dummy parameter nodes using make_agg_arg to represent the expected argument types

## Simplified Source

```c
void
build_aggregate_deserialfn_expr(Oid deserialfn_oid, Expr **deserialfnexpr)
{
    // Build argument list for deserialization function
    // Always takes BYTEA (serialized data) and INTERNAL (context)
    List *args = list_make2(make_agg_arg(BYTEAOID, InvalidOid),
                           make_agg_arg(INTERNALOID, InvalidOid));

    // Create function expression that returns INTERNAL state
    FuncExpr *fexpr = makeFuncExpr(deserialfn_oid,
                                  INTERNALOID,        // return type
                                  args,
                                  InvalidOid,         // inputcollid
                                  InvalidOid,         // funccollid
                                  COERCE_EXPLICIT_CALL);

    *deserialfnexpr = (Expr *) fexpr;
}
```