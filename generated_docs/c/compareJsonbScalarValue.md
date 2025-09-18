# compareJsonbScalarValue

## Location
src/backend/utils/adt/jsonb_util.c: 1439 - 1483

## Overview
compareJsonbScalarValue performs three-way comparison between two JsonbValue scalar values, returning -1, 0, or 1 for use in B-tree operations and sorting.

## Definition


## Detailed Description
This static function implements lexicographic ordering comparison for JsonbValue scalar types, designed specifically for B-tree operators where consistent sort ordering is required. It handles type-specific comparison logic: null values are always equal (return 0), string values are compared using varstr_cmp with the default collation for proper lexical ordering, numeric values use PostgreSQL's numeric_cmp function to ensure mathematically correct ordering, and boolean values are compared with false < true ordering. The function enforces type safety by requiring identical types and generating errors for mismatched or invalid types.

## Parameters / Member Variables
- : Pointer to the first JsonbValue scalar for comparison
- : Pointer to the second JsonbValue scalar for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [varstr_cmp](../v/varstr_cmp.md) (for string comparison with collation)
  - DirectFunctionCall2 (for calling numeric_cmp)
  - numeric_cmp (for numeric comparison)
  - [PointerGetDatum](../P/PointerGetDatum.md) (for datum conversion)
  - [DatumGetInt32](../D/DatumGetInt32.md) (for integer result conversion)
  - DEFAULT_COLLATION_OID (collation constant)
- Called from (representative examples):
  - [compareJsonbContainers](compareJsonbContainers.md)

## Notes and Other Information
The function is declared static and limited to jsonb_util.c scope. It provides consistent ordering semantics required for B-tree indexing and sorting operations on JSONB data. String comparisons respect PostgreSQL's default collation rules, ensuring proper locale-aware sorting. Boolean comparison follows the convention that false (0) is less than true (1). The function will generate ERROR conditions for type mismatches or invalid scalar types, maintaining type safety in comparison operations.