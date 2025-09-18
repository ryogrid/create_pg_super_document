# tupledesc_match

## Location
src/backend/executor/execSRF.c: 943 - 980

## Overview
tupledesc_match validates that a function's returned tuple descriptor matches the expected tuple descriptor from the query context, ensuring type compatibility and proper error reporting for mismatches.

## Definition
```c
static void tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc)
```

## Detailed Description
This function performs comprehensive validation to ensure that the tuple descriptor returned by a function matches what the query context expects. It serves as a critical type-safety mechanism in PostgreSQL's set-returning function execution framework. The function validates both the structural aspects (number of attributes) and type compatibility of each attribute.

The validation process is sophisticated, using binary coercion rules to determine type compatibility rather than requiring exact type matches. For dropped columns in the destination descriptor, the function only enforces physical storage compatibility (attribute length and alignment), allowing for some flexibility with cached plans that may be out of date.

When mismatches are detected, the function generates detailed error messages that help users understand exactly what went wrong, including specific information about attribute counts, type mismatches, and physical storage incompatibilities.

## Parameters / Member Variables
- `dst_tupdesc`: The expected tuple descriptor from the query context (destination)
- `src_tupdesc`: The tuple descriptor returned by the function (source)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro to access tuple descriptor attributes)
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md) (to check type compatibility through binary coercion)
  - ereport (for error reporting)
  - [errdetail_plural](../e/errdetail_plural.md) (for pluralized error details)
  - [format_type_be](../f/format_type_be.md) (to format type names in error messages)
- Called from (representative examples):
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md) (for table function result validation)
  - [ExecPrepareTuplestoreResult](../E/ExecPrepareTuplestoreResult.md) (for tuplestore-based result validation)

## Notes and Other Information
- This is a static function, only accessible within the execSRF.c compilation unit
- The function prioritizes attribute count validation first, providing clear error messages about count mismatches
- Uses binary coercion rules rather than strict type equality, allowing for compatible type conversions
- Special handling for dropped columns - only validates physical storage characteristics for these
- Comprehensive error reporting includes ordinal positions and specific type information to aid in debugging
- The validation supports scenarios with out-of-date cached plans by being flexible about dropped column types
- Physical storage validation (attlen and attalign) ensures that even dropped columns maintain storage compatibility