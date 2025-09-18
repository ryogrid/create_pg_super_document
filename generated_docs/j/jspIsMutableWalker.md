# jspIsMutableWalker

## Location
src/backend/utils/adt/jsonpath.c: 1294 - 1522

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
  - JsonPathMutableContext (structure type) 
  - JsonPathDatatypeStatus (enum type)
  - jspGetArg, jspGetLeftArg, jspGetRightArg (argument accessors)
  - jspGetString (string extraction)
  - jspGetArraySubscript (array subscript extraction)
  - jspGetNext (next item traversal)
  - jspInitByBuffer (buffer initialization)
  - datetime_format_has_tz (timezone detection utility)
  - Various jpi* enum constants for different JSON path item types
- Called from (representative examples):
  - jspIsMutable (main entry point)
  - jspIsMutableWalker (recursive self-calls for tree traversal)

## Notes and Other Information
- This is a static function used internally by the JSON path mutability detection system
- The function is recursive and can call itself multiple times for complex expressions with nested structures
- Returns JsonPathDatatypeStatus to indicate the datetime context of the analyzed expression
- Mutability detection focuses primarily on datetime operations and timezone-dependent comparisons
- The walker respects LAX/STRICT mode settings which can affect mutability determination for certain operations
- Essential for PostgreSQL's query optimization, allowing the planner to make informed decisions about expression caching and evaluation strategies