# getScalar

## Location
src/backend/utils/adt/jsonpath_exec.c: 3638 - 3648

## Overview
getScalar is a static utility function that validates and returns a JsonbValue if it matches a specified scalar type, or returns NULL if there's a type mismatch.

## Definition
static JsonbValue *getScalar(JsonbValue *scalar, enum jbvType type)

## Detailed Description
This function performs type checking for scalar JsonbValue objects. It verifies that the provided JsonbValue matches the expected scalar type and returns the value if there's a match, or NULL if the types don't match. The function includes an assertion to ensure that binary containers containing scalar values are not passed to it, as scalar values should have been extracted from binary containers earlier in the JSON path execution process. This function is commonly used throughout the JSON path execution engine when operations need to work with specific scalar types like strings, numbers, or booleans.

## Parameters / Member Variables
- `scalar`: Pointer to the JsonbValue to check and potentially return
- `type`: The expected jbvType enum value that the scalar should match

## Dependencies
- Functions called/Symbols referenced:
  - jbvType (enum defining JsonbValue types)
  - jbvBinary (JsonbValue type constant for binary data)
  - JsonContainerIsScalar (macro to check if container holds a scalar value)
- Called from (representative examples):
  - executeBinaryArithmExpr (for numeric operations)
  - executeUnaryArithmExpr (for unary arithmetic)
  - executeStartsWith (for string operations)
  - executeLikeRegex (for pattern matching)
  - executeNumericItemMethod (for numeric methods)
  - executeDateTimeMethod (for date/time operations)
  - getArrayIndex (for array indexing)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c, not exposed in the public API
- Includes an assertion that prevents binary scalar containers from being processed, as they should be extracted earlier
- Returns the original JsonbValue pointer if type matches, NULL otherwise
- Simple but crucial function for type safety in JSON path operations
- Used extensively in arithmetic, string, and other type-specific operations
- The function provides a clean interface for conditional type checking without throwing errors