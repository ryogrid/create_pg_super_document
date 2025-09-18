# operationPriority

## Location
src/backend/utils/adt/jsonpath.c: 917 - 953

## Overview
Returns the precedence level of JSON path operations to ensure correct operator precedence during expression evaluation and formatting.

## Definition


## Detailed Description
This static function assigns numeric priority values to JSON path operations according to standard mathematical and logical operator precedence rules. Lower numbers indicate higher precedence (higher priority). The function is used primarily during JSON path expression printing and parsing to determine when parentheses are needed to preserve the intended order of operations. The precedence follows conventional operator precedence: unary operators (highest), multiplicative operators, additive operators, comparison operators, logical AND, and logical OR (lowest).

## Parameters / Member Variables
- : A JsonPathItemType enumeration value representing the JSON path operation whose precedence level is requested

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItemType (enum parameter)
  - jpi* enumeration constants (jpiOr, jpiAnd, jpiEqual, jpiAdd, jpiMul, jpiPlus, etc.)
- Called from (representative examples):
  - [printJsonPathItem](../p/printJsonPathItem.md) (multiple locations for precedence checking and parentheses placement)

## Notes and Other Information
- Precedence levels from 0 (lowest) to 6 (highest): OR(0), AND(1), comparisons(2), addition/subtraction(3), multiplication/division/modulo(4), unary plus/minus(5), default(6)
- Static function, only accessible within the same source file
- Essential for correct parenthesization in JSON path expression string representation
- Comparison operators and 'starts with' share the same precedence level (2)
- Binary arithmetic operations have different precedence than their unary counterparts (jpiAdd vs jpiPlus, jpiSub vs jpiMinus)