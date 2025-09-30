# jspIsMutableWalker

## Location
[src/backend/utils/adt/jsonpath.c:1294-1522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L1294-L1522)

## Overview
A recursive tree walker that traverses JSON path expression nodes to detect mutable operations and track data type status for mutability analysis.

## Definition
```c
static enum JsonPathDatatypeStatus jspIsMutableWalker(JsonPathItem *jpi, struct JsonPathMutableContext *cxt)
```

## Detailed Description
This function implements the core logic for detecting mutability in JSON path expressions by recursively walking through the JSON path item tree. It analyzes each node type and determines whether the operations contained within could produce different results on successive evaluations with the same inputs.

The walker maintains a JsonPathDatatypeStatus to track whether the current evaluation context involves datetime operations, which can be mutable depending on timezone handling. It processes various JSON path item types including literals, operators, accessors, methods, and special constructs like filters and array subscripts.

Key mutability detection includes:
- Datetime operations that depend on current time (jpiTime, jpiDate, jpiTimestamp, etc.)
- Datetime comparisons between different timezone contexts
- Variable references with datetime types
- Array access in non-LAX mode

The function continues traversing until either a mutable operation is detected (setting cxt->mutable = true) or the entire expression has been analyzed. It handles both simple expressions and complex nested structures with appropriate context switching.

## Parameters / Member Variables
- `jpi`: JsonPathItem pointer representing the current node in the JSON path expression tree
- `cxt`: JsonPathMutableContext structure containing analysis state including variable information, current datetime status, LAX mode flag, and mutability result

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItem (structure type)
  - [JsonPathMutableContext](../J/JsonPathMutableContext.md) (structure type) 
  - JsonPathDatatypeStatus (enum type)
  - [jspGetArg](jspGetArg.md), jspGetLeftArg, jspGetRightArg (argument accessors)
  - [jspGetString](jspGetString.md) (string extraction)
  - [jspGetArraySubscript](jspGetArraySubscript.md) (array subscript extraction)
  - [jspGetNext](jspGetNext.md) (next item traversal)
  - [jspInitByBuffer](jspInitByBuffer.md) (buffer initialization)
  - [datetime_format_has_tz](../d/datetime_format_has_tz.md) (timezone detection utility)
  - Various jpi* enum constants for different JSON path item types
- Called from (representative examples):
  - [jspIsMutable](jspIsMutable.md) (main entry point)
  - [jspIsMutableWalker](jspIsMutableWalker.md) (recursive self-calls for tree traversal)

## Notes and Other Information
- This is a static function used internally by the JSON path mutability detection system
- The function is recursive and can call itself multiple times for complex expressions with nested structures
- Returns JsonPathDatatypeStatus to indicate the datetime context of the analyzed expression
- Mutability detection focuses primarily on datetime operations and timezone-dependent comparisons
- The walker respects LAX/STRICT mode settings which can affect mutability determination for certain operations
- Essential for PostgreSQL's query optimization, allowing the planner to make informed decisions about expression caching and evaluation strategies

## Simplified Source

```c
static enum JsonPathDatatypeStatus
jspIsMutableWalker(JsonPathItem *jpi, struct JsonPathMutableContext *cxt)
{
    JsonPathItem next;
    enum JsonPathDatatypeStatus status = jpdsNonDateTime;

    // Walk through JSON path items until mutable operation found
    while (!cxt->mutable)
    {
        JsonPathItem arg;
        enum JsonPathDatatypeStatus leftStatus, rightStatus;

        switch (jpi->type)
        {
            case jpiRoot:
                // Root node is non-datetime
                break;

            case jpiCurrent:
                // Current context determines status
                status = cxt->current;
                break;

            case jpiFilter:
                // Process filter with current context
                cxt->current = status;
                jspGetArg(jpi, &arg);
                jspIsMutableWalker(&arg, cxt);
                break;

            case jpiVariable:
                // Check variable types for datetime status
                status = check_variable_datetime_type(jpi, cxt);
                break;

            case jpiEqual:
            case jpiNotEqual:
            case jpiLess:
            case jpiGreater:
            case jpiLessOrEqual:
            case jpiGreaterOrEqual:
                // Compare datetime types - different timezones = mutable
                leftStatus = jspIsMutableWalker(left_arg, cxt);
                rightStatus = jspIsMutableWalker(right_arg, cxt);
                if (datetime_comparison_is_mutable(leftStatus, rightStatus))
                    cxt->mutable = true;
                break;

            case jpiDatetime:
                // Datetime operations are potentially mutable
                if (has_timezone_in_template())
                    status = jpdsDateTimeZoned;
                else
                    status = jpdsDateTimeNonZoned;
                break;

            case jpiTime:
            case jpiDate:
            case jpiTimestamp:
            case jpiTimeTz:
            case jpiTimestampTz:
                // These functions depend on current time - always mutable
                cxt->mutable = true;
                status = jpdsDateTimeNonZoned;
                break;

            // Various other operations handled...
        }

        // Move to next item in chain
        if (!jspGetNext(jpi, &next))
            break;
        jpi = &next;
    }

    return status;
}
```