# jspGetLeftArg

## Location
[src/backend/utils/adt/jsonpath.c:1159-1180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L1159-L1180)

## Overview
Extracts and initializes the left argument from a binary JSON path operation item.

## Definition

```c
void
jspGetLeftArg(JsonPathItem *v, JsonPathItem *a)
```
## Detailed Description
The jspGetLeftArg function is designed to extract the left operand from binary JSON path operations. It validates that the input JsonPathItem represents a binary operation through an Assert statement that checks for logical operations (AND, OR), comparison operations (equal, not equal, less than, greater than, etc.), arithmetic operations (add, subtract, multiply, divide, modulo), string operations (starts with), and type conversion operations (decimal). Once validated, it initializes the destination JsonPathItem structure with the left argument using jspInitByBuffer(), accessing the left argument offset stored in the item's content.args.left field.

## Parameters / Member Variables
- `*v`: Pointer to the source JsonPathItem containing the binary operation
- `*a`: Pointer to the destination JsonPathItem to be initialized with the left argument
## Dependencies
- Functions called/Symbols referenced:
  - [jspInitByBuffer](jspInitByBuffer.md)
  - JsonPathItem (struct type)
  - Binary operation type constants (jpiAnd, jpiOr, jpiEqual, jpiNotEqual, jpiLess, jpiGreater, jpiLessOrEqual, jpiGreaterOrEqual, jpiAdd, jpiSub, jpiMul, jpiDiv, jpiMod, jpiStartsWith, jpiDecimal)
- Called from (representative examples):
  - [extract_jsp_bool_expr](../e/extract_jsp_bool_expr.md)
  - [printJsonPathItem](../p/printJsonPathItem.md)
  - [jspIsMutableWalker](jspIsMutableWalker.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeBoolItem](../e/executeBoolItem.md)
  - [executeBinaryArithmExpr](../e/executeBinaryArithmExpr.md)

## Notes and Other Information
- This function does not return a value (void return type)
- The Assert statement serves as both validation and documentation of supported binary operation types
- Works in conjunction with jspGetRightArg to access both operands of binary operations
- Essential for JSON path expression evaluation and manipulation in PostgreSQL's JSON processing engine
- The function assumes the caller has already verified that the item has binary arguments

## Simplified Source

```c
void jspGetLeftArg(JsonPathItem *v, JsonPathItem *a) {
    // Validate that this is a binary operation type
    Assert(v->type == jpiAnd ||
           v->type == jpiOr ||
           v->type == jpiEqual ||
           v->type == jpiNotEqual ||
           v->type == jpiLess ||
           v->type == jpiGreater ||
           v->type == jpiLessOrEqual ||
           v->type == jpiGreaterOrEqual ||
           v->type == jpiAdd ||
           v->type == jpiSub ||
           v->type == jpiMul ||
           v->type == jpiDiv ||
           v->type == jpiMod ||
           v->type == jpiStartsWith ||
           v->type == jpiDecimal);

    // Initialize the left argument JsonPathItem from the buffer
    jspInitByBuffer(a, v->base, v->content.args.left);
}
```