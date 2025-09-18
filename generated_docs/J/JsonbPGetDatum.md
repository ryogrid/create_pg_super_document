# JsonbPGetDatum

## Location
src/include/utils/jsonb.h: 386 - 390

## Overview
JsonbPGetDatum is a convenience macro function that converts a Jsonb pointer to a Datum value for use in PostgreSQL's type system.

## Definition


## Detailed Description
This inline function provides a convenient way to convert a Jsonb pointer back to a Datum value, which is the universal data type used throughout PostgreSQL's internal type system. The function is essentially a wrapper around PointerGetDatum that provides type safety and clarity when working with JSONB values. It serves as the reverse operation of DatumGetJsonbP, enabling the conversion from Jsonb structures back to Datum format for storage, function return values, and inter-module communication within PostgreSQL.

## Parameters / Member Variables
- : A const pointer to a Jsonb structure that needs to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - Jsonb (type)
- Called from (representative examples):
  - [ExecEvalJsonExprPath](../E/ExecEvalJsonExprPath.md)
  - [ExecGetJsonValueItemString](../E/ExecGetJsonValueItemString.md)
  - [datum_to_jsonb](../d/datum_to_jsonb.md)
  - [jsonb_build_object_worker](../j/jsonb_build_object_worker.md)
  - [jsonb_build_array_worker](../j/jsonb_build_array_worker.md)
  - [populate_scalar](../p/populate_scalar.md)
  - [jsonb_path_query_internal](../j/jsonb_path_query_internal.md)
  - [JsonItemFromDatum](JsonItemFromDatum.md)
  - [JsonPathQuery](JsonPathQuery.md)
  - [JsonTablePlanScanNextRow](JsonTablePlanScanNextRow.md)

## Notes and Other Information
- This is a static inline function defined in src/include/utils/jsonb.h
- Provides the reverse conversion of DatumGetJsonbP, completing the type conversion cycle
- Used extensively in JSONB function implementations for returning results
- The const qualifier on the parameter indicates that the function does not modify the input Jsonb structure
- Essential for interfacing JSONB operations with PostgreSQL's function call protocol
- Part of the convenience macro family for seamless JSONB integration with PostgreSQL's type system
- Commonly used in path query operations and JSONB construction functions