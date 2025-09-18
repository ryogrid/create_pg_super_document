# jspGetNumeric

## Location
[src/backend/utils/adt/jsonpath.c:1211-1218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L1211-L1218)

## Overview
Extracts and returns the numeric value from a JSON path numeric item.

## Definition


## Detailed Description
The jspGetNumeric function is a type-specific accessor function that extracts numeric values from JSON path items. It first validates that the input JsonPathItem is of type jpiNumeric through an Assert statement to ensure type safety. Once validated, it directly casts and returns the data pointer stored in the item's content.value.data field as a PostgreSQL Numeric type. This function provides safe access to numeric constants and values embedded in JSON path expressions, returning them in PostgreSQL's internal numeric representation format.

## Parameters / Member Variables
- : Pointer to the JsonPathItem containing the numeric value (must be of type jpiNumeric)

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItem (struct type)
  - jpiNumeric (enumeration constant)
  - Numeric (PostgreSQL numeric data type)
- Called from (representative examples):
  - [printJsonPathItem](../p/printJsonPathItem.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)
  - [getJsonPathItem](../g/getJsonPathItem.md)

## Notes and Other Information
- Returns the numeric value as a PostgreSQL Numeric type, not a primitive C numeric type
- The Assert statement provides runtime type checking in debug builds
- Part of a family of type-specific accessor functions alongside jspGetBool, jspGetString, etc.
- Used extensively in JSON path arithmetic operations and numeric comparisons
- The returned Numeric value can be used with PostgreSQL's numeric manipulation functions
- Essential for JSON path expression evaluation involving numeric literals and computed values