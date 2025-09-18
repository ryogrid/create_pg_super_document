# get_expr_result_tupdesc

## Location
src/backend/utils/fmgr/funcapi.c: 551 - 588

## Overview
A simplified function that extracts only the TupleDesc from composite-valued expressions, providing a convenient interface when only the tuple descriptor is needed rather than full type classification.

## Definition
```c
TupleDesc get_expr_result_tupdesc(Node *expr, bool noError)
```

## Detailed Description
This function serves as a streamlined version of `get_expr_result_type` for use cases where the caller is specifically interested in obtaining a TupleDesc for composite expressions and doesn't need the broader type classification functionality. It internally calls `get_expr_result_type` but only returns the TupleDesc portion of the result.

The function specifically handles:
- **TYPEFUNC_COMPOSITE**: Returns the TupleDesc for regular composite types
- **TYPEFUNC_COMPOSITE_DOMAIN**: Returns the TupleDesc for composite domain types
- **Non-composite types**: Either returns NULL (if noError is true) or throws appropriate error messages

Error handling distinguishes between two scenarios:
1. Non-RECORD types that aren't composite: Reports "type X is not composite"
2. RECORD types that couldn't be resolved: Reports "record type has not been registered"

The function includes the same warning as `get_expr_result_type` about being cautious when using it on funcexpr of RTEs with coldeflist.

## Parameters / Member Variables
- `expr`: Expression node to analyze for composite type information
- `noError`: If true, returns NULL on failure; if false, throws error on failure

## Dependencies
- Functions called/Symbols referenced:
  - [get_expr_result_type](get_expr_result_type.md)
  - exprType
  - [format_type_be](../f/format_type_be.md)
  - ereport
  - TYPEFUNC_COMPOSITE
  - TYPEFUNC_COMPOSITE_DOMAIN
- Called from (representative examples):
  - [process_function_rte_ref](../p/process_function_rte_ref.md)
  - [ParseComplexProjection](../P/ParseComplexProjection.md)
  - [get_rte_attribute_is_dropped](get_rte_attribute_is_dropped.md)
  - [ExpandRowReference](../E/ExpandRowReference.md)
  - [expandRecordVariable](../e/expandRecordVariable.md)
  - [get_name_for_var_field](get_name_for_var_field.md)

## Notes and Other Information
- Provides a simpler interface compared to get_expr_result_type when only TupleDesc is needed
- Includes the same RTE coldeflist caveat as get_expr_result_type
- Error handling provides specific messages for different failure scenarios
- Returns NULL for non-composite expressions when noError is true
- Widely used in parser and utility functions that need to work with composite expression structures
- Located in src/backend/utils/fmgr/funcapi.c at lines 551-588
- Part of PostgreSQL's function manager API for composite type introspection