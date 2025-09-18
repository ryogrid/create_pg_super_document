# jsonb_subscript_transform

## Location
src/backend/utils/adt/jsonbsubs.c: 43 - 174

## Overview
Finishes parse analysis of a SubscriptingRef expression for JSONB by transforming subscript expressions, coercing them to appropriate types, and determining the result type.

## Definition


## Detailed Description
This function handles the transformation phase of JSONB subscripting operations during SQL parsing. It processes each subscript expression in the indirection list, validates that slicing is not used (which is unsupported for JSONB), and coerces subscript expressions to either integer or text types. The function implements type disambiguation logic to ensure that subscripts can only be coerced to one target type, preventing ambiguous subscript operations similar to overloaded function resolution.

The transformation process includes:
1. Iterating through all subscript expressions in the indirection list
2. Rejecting slice operations with appropriate error messages
3. Determining whether each subscript can be coerced to int4 or text
4. Ensuring no ambiguous coercion scenarios (subscript coercible to multiple types)
5. Performing the actual type coercion
6. Setting the result type to JSONBOID

## Parameters / Member Variables
- : The SubscriptingRef node being transformed, which will be updated with processed subscript expressions
- : List of A_Indices nodes representing the subscript expressions to be processed
- : Parse state containing context information for error reporting and expression transformation
- : Boolean indicating if this is a slice operation (always results in error for JSONB)
- : Boolean indicating if this subscripting is part of an assignment operation

## Dependencies
- Functions called/Symbols referenced:
  - transformExpr
  - exprType
  - can_coerce_type
  - coerce_type
  - format_type_be
  - exprLocation
  - ereport
- Called from:
  - jsonb_subscript_handler

## Notes and Other Information
- JSONB subscripting does not support slice operations and will generate errors if attempted
- Subscripts must be coercible to either integer (for array indexing) or text (for object key access)
- The function implements strict type disambiguation - if a subscript type can be coerced to both int4 and text, it generates an error
- Uses implicit coercion context similar to overloaded function resolution
- Always sets the result type to JSONBOID regardless of the subscript types used
- Error messages include parser position information for better user experience