# get_json_agg_constructor

## Location
[src/backend/utils/adt/ruleutils.c:11438-11462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11438-L11462)

## Overview
A static helper function that deparses aggregate JSON constructor expressions, handling both aggregate functions and window functions with JSON constructor semantics.

## Definition
```c
static void get_json_agg_constructor(JsonConstructorExpr *ctor, deparse_context *context, 
                                   const char *funcname, bool is_json_objectagg)
```

## Detailed Description
This function is responsible for deparsing aggregate JSON constructor expressions back into their SQL text representation. It handles JsonConstructorExpr nodes that contain either Aggref (aggregate function) or WindowFunc (window function) nodes as their underlying function implementation.

The function first collects JSON constructor options using get_json_constructor_options(), then delegates to appropriate helper functions based on the underlying node type:
- For Aggref nodes: calls get_agg_expr_helper()
- For WindowFunc nodes: calls get_windowfunc_expr_helper() 
- For any other node type: raises an error

This function is part of PostgreSQL's rule deparsing system, which converts internal query tree representations back into readable SQL text.

## Parameters / Member Variables
- `ctor`: Pointer to the JsonConstructorExpr structure containing the constructor information
- `context`: Deparsing context containing information about the current deparsing operation
- `funcname`: String representing the function name to use in the deparsed output
- `is_json_objectagg`: Boolean flag indicating whether this is a JSON_OBJECTAGG function

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md) (for StringInfo initialization)
  - [get_json_constructor_options](get_json_constructor_options.md) (for collecting JSON options)
  - [get_agg_expr_helper](get_agg_expr_helper.md) (for aggregate function deparsing)
  - [get_windowfunc_expr_helper](get_windowfunc_expr_helper.md) (for window function deparsing)
  - IsA (for type checking macros)
  - nodeTag (for node type identification)
  - elog (for error reporting)
- Types referenced:
  - [JsonConstructorExpr](../J/JsonConstructorExpr.md)
  - [deparse_context](../d/deparse_context.md)
  - [Aggref](../A/Aggref.md)
  - [WindowFunc](../W/WindowFunc.md)
- Called from:
  - [get_json_constructor](get_json_constructor.md) (for JSON_OBJECTAGG and JSON_ARRAYAGG cases)

## Notes and Other Information
- This is a static function within ruleutils.c used exclusively for rule deparsing operations
- The function supports both aggregate and window function variants of JSON constructors
- Error handling is implemented for unsupported underlying node types
- The is_json_objectagg parameter is passed through to helper functions to enable specialized handling for JSON object aggregation vs array aggregation
- StringInfo is used for building the options string that gets passed to the helper functions

## Simplified Source

```c
static void get_json_agg_constructor(JsonConstructorExpr *ctor, deparse_context *context,
                                   const char *funcname, bool is_json_objectagg) {
    StringInfoData options;

    // Collect JSON constructor options
    initStringInfo(&options);
    get_json_constructor_options(ctor, &options);

    // Delegate to appropriate helper based on function type
    if (IsA(ctor->func, Aggref)) {
        // Handle aggregate functions like JSON_ARRAYAGG, JSON_OBJECTAGG
        get_agg_expr_helper((Aggref *) ctor->func, context,
                           (Aggref *) ctor->func,
                           funcname, options.data, is_json_objectagg);
    } else if (IsA(ctor->func, WindowFunc)) {
        // Handle window function versions
        get_windowfunc_expr_helper((WindowFunc *) ctor->func, context,
                                  funcname, options.data, is_json_objectagg);
    } else {
        // Unsupported underlying node type
        elog(ERROR, "invalid JsonConstructorExpr underlying node type: %d",
             nodeTag(ctor->func));
    }
}
```