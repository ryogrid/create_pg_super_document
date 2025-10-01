# jspOperationName

## Location
[src/backend/utils/adt/jsonpath.c:836-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L836-L916)

## Overview
Returns the string representation of a JSON path operation based on its JsonPathItemType, used for debugging and error reporting in JSON path expression processing.

## Definition

```c
const char *
jspOperationName(JsonPathItemType type)
```
## Detailed Description
This function provides a mapping from JsonPathItemType enumeration values to their corresponding string representations. It serves as a utility function primarily used for debugging, error messages, and display purposes when working with JSON path expressions. The function handles all the various operation types supported in PostgreSQL's JSON path implementation, including arithmetic operations (+, -, *, /, %), comparison operations (==, \!=, <, >, <=, >=), logical operations (&&, ||), and various built-in functions and type conversion operations.

## Parameters / Member Variables
- : A JsonPathItemType enumeration value representing the specific JSON path operation or function to be converted to a string representation

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItemType (enum parameter)
  - All jpi* enumeration constants (jpiAnd, jpiOr, jpiEqual, etc.)
  - elog (for error reporting)
- Called from (representative examples):
  - [printJsonPathItem](../p/printJsonPathItem.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeBinaryArithmExpr](../e/executeBinaryArithmExpr.md)
  - [executeUnaryArithmExpr](../e/executeUnaryArithmExpr.md)
  - [executeNumericItemMethod](../e/executeNumericItemMethod.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)

## Notes and Other Information
- The function uses a comprehensive switch statement covering all JsonPathItemType values
- Returns NULL and logs an ERROR if an unrecognized type is passed
- Some operations like jpiAdd/jpiPlus and jpiSub/jpiMinus share the same string representation
- Covers arithmetic, comparison, logical operations, and various JSON type conversion and utility functions
- Essential for error reporting and debugging in JSON path expression evaluation

## Simplified Source

```c
const char *
jspOperationName(JsonPathItemType type)
{
    switch (type)
    {
        // Logical operations
        case jpiAnd:           return "&&";
        case jpiOr:            return "||";

        // Comparison operations
        case jpiEqual:         return "==";
        case jpiNotEqual:      return "!=";
        case jpiLess:          return "<";
        case jpiGreater:       return ">";
        case jpiLessOrEqual:   return "<=";
        case jpiGreaterOrEqual: return ">=";

        // Arithmetic operations
        case jpiAdd:
        case jpiPlus:          return "+";
        case jpiSub:
        case jpiMinus:         return "-";
        case jpiMul:           return "*";
        case jpiDiv:           return "/";
        case jpiMod:           return "%";

        // Built-in functions
        case jpiType:          return "type";
        case jpiSize:          return "size";
        case jpiAbs:           return "abs";
        case jpiFloor:         return "floor";
        case jpiCeiling:       return "ceiling";
        case jpiDouble:        return "double";
        case jpiDatetime:      return "datetime";
        case jpiKeyValue:      return "keyvalue";
        case jpiStartsWith:    return "starts with";
        case jpiLikeRegex:     return "like_regex";

        // Type conversion functions
        case jpiBigint:        return "bigint";
        case jpiBoolean:       return "boolean";
        case jpiDate:          return "date";
        case jpiDecimal:       return "decimal";
        case jpiInteger:       return "integer";
        case jpiNumber:        return "number";
        case jpiStringFunc:    return "string";
        case jpiTime:          return "time";
        case jpiTimeTz:        return "time_tz";
        case jpiTimestamp:     return "timestamp";
        case jpiTimestampTz:   return "timestamp_tz";

        default:
            elog(ERROR, "unrecognized jsonpath item type: %d", type);
            return NULL;
    }
}
```