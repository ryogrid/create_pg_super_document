# make_agg_arg

## Location
src/backend/parser/parse_agg.c: 2183 - 2194

## Overview
Creates dummy parameter expressions for aggregate functions that allow support functions to discover their actual argument types at runtime.

## Definition
```c
static Node *make_agg_arg(Oid argtype, Oid argcollation)
```

## Detailed Description
This is a utility function that constructs dummy Param nodes for use in aggregate function expressions. These dummy parameters do not correspond to any real query parameters but serve as placeholders that carry type information. The primary purpose is to enable aggregate support functions to determine their actual argument types at runtime using `get_fn_expr_argtype()`. This approach provides a clean way to pass type metadata through the expression system without requiring complex type lookup mechanisms.

The function creates PARAM_EXEC type parameters with a fixed paramid of -1, indicating they are not bound to actual execution parameters but are purely for type information purposes.

## Parameters / Member Variables
- `argtype`: The OID of the data type this dummy argument should represent
- `argcollation`: The OID of the collation for this argument type

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - Param (struct type)
  - PARAM_EXEC (constant)
- Called from (representative examples):
  - build_aggregate_transfn_expr
  - build_aggregate_serialfn_expr
  - build_aggregate_deserialfn_expr
  - build_aggregate_finalfn_expr

## Notes and Other Information
- Returns a static function (internal to parse_agg.c)
- Uses paramid = -1 to indicate this is not a real parameter
- Uses PARAM_EXEC as the parameter kind
- Sets paramtypmod = -1 and location = -1 as defaults
- Essential for aggregate function type resolution at runtime
- Located in src/backend/parser/parse_agg.c:2183-2194
- Widely used across all aggregate expression building functions