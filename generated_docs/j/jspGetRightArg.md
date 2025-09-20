# jspGetRightArg

## Location
[src/backend/utils/adt/jsonpath.c:1181-1202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L1181-L1202)

## Overview
Extracts and initializes the right argument from a binary JSON path operation item.

## Definition

```c
void
jspGetRightArg(JsonPathItem *v, JsonPathItem *a)
```
## Detailed Description
The jspGetRightArg function is the counterpart to jspGetLeftArg, designed to extract the right operand from binary JSON path operations. It performs identical validation to jspGetLeftArg, using an Assert statement to verify that the input JsonPathItem represents a binary operation including logical operations (AND, OR), comparison operations (equal, not equal, less than, greater than, etc.), arithmetic operations (add, subtract, multiply, divide, modulo), string operations (starts with), and type conversion operations (decimal). Once validated, it initializes the destination JsonPathItem structure with the right argument using jspInitByBuffer(), accessing the right argument offset stored in the item's content.args.right field.

## Parameters / Member Variables
- : Pointer to the source JsonPathItem containing the binary operation
- : Pointer to the destination JsonPathItem to be initialized with the right argument

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
- Shares identical validation logic with jspGetLeftArg for consistency
- Essential companion function to jspGetLeftArg for complete binary operation argument extraction
- Used extensively in JSON path expression evaluation and manipulation throughout PostgreSQL's JSON processing engine
- The function accesses content.args.right instead of content.args.left to retrieve the second operand of binary operations