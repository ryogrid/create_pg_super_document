# jspGetArg

## Location
[src/backend/utils/adt/jsonpath.c:1074-1091](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L1074-L1091)

## Overview
Retrieves the single argument of unary JSON path operations by initializing a JsonPathItem structure with the argument data.

## Definition

```c
void
jspGetArg(JsonPathItem *v, JsonPathItem *a)
```
## Detailed Description
This function extracts the single argument from unary JSON path operations that store their operand in the content.arg field. It validates that the provided JsonPathItem represents a unary operation (one that takes exactly one argument) and then initializes the argument JsonPathItem by calling jspInitByBuffer with the argument's buffer position. The function is specifically designed for operations like logical NOT, unary plus/minus, filters, existence checks, and various datetime conversion functions that all follow the single-argument pattern.

## Parameters / Member Variables
- : Pointer to the JsonPathItem containing the unary operation whose argument should be extracted
- : Pointer to the JsonPathItem structure to be initialized with the argument data

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItem (struct type)
  - jpi* enumeration constants for unary operations (jpiNot, jpiIsUnknown, jpiPlus, jpiMinus, jpiFilter, jpiExists, jpiDatetime, jpiTime, jpiTimeTz, jpiTimestamp, jpiTimestampTz)
  - [jspInitByBuffer](jspInitByBuffer.md) (core buffer initialization function)
  - Assert (debugging macro for validation)
- Called from (representative examples):
  - [extract_jsp_path_expr_nodes](../e/extract_jsp_path_expr_nodes.md), extract_jsp_bool_expr
  - [printJsonPathItem](../p/printJsonPathItem.md) (multiple locations)
  - [jspIsMutableWalker](jspIsMutableWalker.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeBoolItem](../e/executeBoolItem.md)
  - [executeUnaryArithmExpr](../e/executeUnaryArithmExpr.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)

## Notes and Other Information
- Only works with unary operations that store their single argument in the content.arg field
- The Assert statement ensures type safety by validating that only supported unary operation types are passed
- Essential for traversing and evaluating JSON path expression trees
- Used extensively in both compilation and execution phases of JSON path processing
- Provides type-safe access to unary operation arguments across the entire JSON path system

## Simplified Source

```c
void jspGetArg(JsonPathItem *v, JsonPathItem *a) {
    // Validate that this is a unary operation type
    Assert(v->type == jpiNot ||
           v->type == jpiIsUnknown ||
           v->type == jpiPlus ||
           v->type == jpiMinus ||
           v->type == jpiFilter ||
           v->type == jpiExists ||
           v->type == jpiDatetime ||
           v->type == jpiTime ||
           v->type == jpiTimeTz ||
           v->type == jpiTimestamp ||
           v->type == jpiTimestampTz);

    // Initialize the argument JsonPathItem from the buffer
    jspInitByBuffer(a, v->base, v->content.arg);
}
```