# executeItemOptUnwrapTarget

## Location
[src/backend/utils/adt/jsonpath_exec.c:747-1673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L747-L1673)

## Overview
Core JSONPath execution engine that recursively processes individual JSONPath items against JSONB values, with configurable array unwrapping behavior to handle PostgreSQL's JSONPath semantics.

## Definition
```c
static JsonPathExecResult executeItemOptUnwrapTarget(JsonPathExecContext *cxt, 
                                                    JsonPathItem *jsp,
                                                    JsonbValue *jb, 
                                                    JsonValueList *found, 
                                                    bool unwrap)
```

## Detailed Description
This function represents the heart of PostgreSQL's JSONPath execution system. It implements a comprehensive dispatcher that handles all JSONPath operations through a large switch statement covering the entire JSONPath specification. The function recursively processes JSONPath expressions by:

1. **Path Navigation**: Handling object key access (`.key`), array indexing (`[0]`), wildcard operators (`.*`, `[*]`)
2. **Value Operations**: Processing literals, variables, and type conversions 
3. **Boolean Logic**: Executing comparison operators, logical operations, and filters
4. **Arithmetic**: Supporting mathematical operations (+, -, *, /, %, unary operators)
5. **Method Calls**: Implementing JSONPath methods like `.size()`, `.type()`, `.abs()`, datetime functions
6. **Type Conversions**: Converting between JSON types (`.string()`, `.number()`, `.boolean()`, etc.)
7. **Structural Operations**: Managing context with `@` (current) and `$` (root) references

The unwrap parameter controls whether single-element arrays are automatically unwrapped in lax mode, which is essential for PostgreSQL's SQL/JSON compliance.

## Parameters / Member Variables
- `cxt`: JSONPath execution context containing mode settings, variables, and state
- `jsp`: Current JSONPath item/operation to execute  
- `jb`: Current JSONB value being processed
- `found`: Output list for collecting matching results (NULL for existence checks)
- `unwrap`: Boolean controlling automatic array unwrapping behavior

## Dependencies
- Functions called/Symbols referenced:
  - [JsonPathExecContext](../J/JsonPathExecContext.md) (execution context structure)
  - JsonPathItem (path item structure)
  - [JsonbValue](../J/JsonbValue.md) (JSONB value representation)
  - [JsonValueList](../J/JsonValueList.md) (result collection)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (execution result enumeration)
  - Multiple JSONPath operation handlers:
    - [executeBoolItem](executeBoolItem.md) (boolean operations)
    - [executeBinaryArithmExpr](executeBinaryArithmExpr.md) (arithmetic operations)
    - [executeNumericItemMethod](executeNumericItemMethod.md) (numeric methods)
    - [executeDateTimeMethod](executeDateTimeMethod.md) (datetime operations)
    - [executeKeyValueMethod](executeKeyValueMethod.md) (key-value operations)
    - [executeAnyItem](executeAnyItem.md) (recursive descent operations)
    - [executeItemUnwrapTargetArray](executeItemUnwrapTargetArray.md) (array processing)
    - [executeNextItem](executeNextItem.md) (continuation processing)
  - Utility functions:
    - [jspGetNext](../j/jspGetNext.md), jspGetArg, jspGetString (JSONPath parsing)
    - [JsonbType](../J/JsonbType.md), JsonbArraySize (JSONB inspection)
    - [getJsonPathItem](../g/getJsonPathItem.md), findJsonbValueFromContainer (JSONB navigation)
    - Various PostgreSQL type conversion functions
- Called from (representative examples):
  - [executeItem](executeItem.md) (standard wrapper)
  - [executeAnyItem](executeAnyItem.md) (recursive descent)

## Notes and Other Information
- This function implements the complete JSONPath specification as per SQL/JSON standards
- Uses extensive stack depth checking and interrupt handling for safety
- Handles both strict and lax execution modes with different error handling policies
- Manages complex type conversions between JSONB and PostgreSQL native types
- Implements sophisticated error reporting with operation-specific error messages
- Critical performance path for all JSONPath operations in PostgreSQL
- Located in src/backend/utils/adt/jsonpath_exec.c:747-1673
- Supports the complete range of JSONPath operations including advanced features like filters, method calls, and type conversions
- The unwrap parameter enables proper SQL/JSON compliance for automatic value unwrapping
- Maintains execution context for variable resolution and mode management throughout the recursion

## Simplified Source

```c
static JsonPathExecResult executeItemOptUnwrapTarget(JsonPathExecContext *cxt,
                                                    JsonPathItem *jsp,
                                                    JsonbValue *jb,
                                                    JsonValueList *found,
                                                    bool unwrap) {
    JsonPathItem elem;
    JsonPathExecResult res = jperNotFound;

    // Safety checks
    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    switch (jsp->type) {
        // Literal values (null, bool, numeric, string, variable)
        case jpiNull:
        case jpiBool:
        case jpiNumeric:
        case jpiString:
        case jpiVariable:
            {
                JsonbValue *v;
                bool hasNext = jspGetNext(jsp, &elem);

                // Get the literal value
                getJsonPathItem(cxt, jsp, v);

                // Continue processing if there are more path items
                return executeNextItem(cxt, jsp, &elem, v, found, hasNext);
            }

        // Boolean operations
        case jpiAnd:
        case jpiOr:
        case jpiEqual:
        case jpiExists:
            {
                JsonPathBool result = executeBoolItem(cxt, jsp, jb, true);
                return appendBoolResult(cxt, jsp, found, result);
            }

        // Arithmetic operations
        case jpiAdd:
        case jpiSub:
        case jpiMul:
        case jpiDiv:
        case jpiMod:
            return executeBinaryArithmExpr(cxt, jsp, jb, /* operator */, found);

        // Array access [*], [n], [n:m]
        case jpiAnyArray:
        case jpiIndexArray:
            if (JsonbType(jb) == jbvArray) {
                // Process array elements based on subscript/wildcard
                return executeItemUnwrapTargetArray(cxt, &elem, jb, found, unwrap);
            }
            break;

        // Object key access (.key, .*)
        case jpiAnyKey:
        case jpiKey:
            if (JsonbType(jb) == jbvObject) {
                // Navigate to object members
                return executeAnyItem(cxt, &elem, jb->val.binary.data, found,
                                    1, 1, 1, false, unwrap);
            }
            break;

        // Type conversion methods (.string(), .number(), etc.)
        case jpiStringFunc:
        case jpiDouble:
        case jpiInteger:
        case jpiDecimal:
        case jpiBoolean:
            {
                JsonbValue converted_value;
                // Convert value to requested type
                convertJsonbValueToType(jb, jsp->type, &converted_value);
                return executeNextItem(cxt, jsp, NULL, &converted_value, found, true);
            }

        // Utility methods (.size(), .type(), .abs(), etc.)
        case jpiSize:
        case jpiType:
        case jpiAbs:
        case jpiFloor:
        case jpiCeiling:
            return executeUtilityMethod(cxt, jsp, jb, found);

        // Context references ($ root, @ current)
        case jpiRoot:
            return executeNextItem(cxt, jsp, NULL, cxt->root, found, true);
        case jpiCurrent:
            return executeNextItem(cxt, jsp, NULL, cxt->current, found, true);

        // Filter expressions (.? condition)
        case jpiFilter:
            {
                JsonPathBool matches = executeNestedBoolItem(cxt, &elem, jb);
                if (matches == jpbTrue)
                    return executeNextItem(cxt, jsp, NULL, jb, found, true);
                return jperNotFound;
            }

        default:
            elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
    }

    return res;
}
```