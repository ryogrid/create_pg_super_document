# jspGetArg

## Location
src/backend/utils/adt/jsonpath.c: 1074 - 1091

## Overview
Retrieves the single argument of unary JSON path operations by initializing a JsonPathItem structure with the argument data.

## Definition


## Detailed Description
This function extracts the single argument from unary JSON path operations that store their operand in the content.arg field. It validates that the provided JsonPathItem represents a unary operation (one that takes exactly one argument) and then initializes the argument JsonPathItem by calling jspInitByBuffer with the argument's buffer position. The function is specifically designed for operations like logical NOT, unary plus/minus, filters, existence checks, and various datetime conversion functions that all follow the single-argument pattern.

## Parameters / Member Variables
- : Pointer to the JsonPathItem containing the unary operation whose argument should be extracted
- : Pointer to the JsonPathItem structure to be initialized with the argument data

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItem (struct type)
  - jpi* enumeration constants for unary operations (jpiNot, jpiIsUnknown, jpiPlus, jpiMinus, jpiFilter, jpiExists, jpiDatetime, jpiTime, jpiTimeTz, jpiTimestamp, jpiTimestampTz)
  - jspInitByBuffer (core buffer initialization function)
  - Assert (debugging macro for validation)
- Called from (representative examples):
  - extract_jsp_path_expr_nodes, extract_jsp_bool_expr
  - printJsonPathItem (multiple locations)
  - jspIsMutableWalker
  - executeItemOptUnwrapTarget
  - executeBoolItem
  - executeUnaryArithmExpr
  - executeDateTimeMethod

## Notes and Other Information
- Only works with unary operations that store their single argument in the content.arg field
- The Assert statement ensures type safety by validating that only supported unary operation types are passed
- Essential for traversing and evaluating JSON path expression trees
- Used extensively in both compilation and execution phases of JSON path processing
- Provides type-safe access to unary operation arguments across the entire JSON path system