# executeItem

## Location
[src/backend/utils/adt/jsonpath_exec.c:735-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L735-L746)

## Overview
Wrapper function that executes a JSONPath item with automatic unwrapping behavior determined by the current execution context's lax mode setting.

## Definition
```c
static JsonPathExecResult executeItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
                                     JsonbValue *jb, JsonValueList *found)
```

## Detailed Description
This function serves as a convenience wrapper around `executeItemOptUnwrapTarget` that automatically determines the unwrapping behavior based on the execution context. In PostgreSQL's JSONPath implementation, "unwrapping" refers to the process of automatically extracting scalar values from single-element arrays or single-property objects when operating in lax mode.

The function delegates the actual execution to `executeItemOptUnwrapTarget` while using `jspAutoUnwrap(cxt)` to determine whether automatic unwrapping should be applied based on the current execution mode and context settings. This abstraction simplifies the common case where the caller wants the standard unwrapping behavior without manually managing the unwrapping decision.

## Parameters / Member Variables
- `cxt`: JSONPath execution context containing mode settings and state
- `jsp`: JSONPath item/expression to execute
- `jb`: Current JSONB value being processed
- `found`: Output list to collect matching results

## Dependencies
- Functions called/Symbols referenced:
  - [JsonPathExecContext](../J/JsonPathExecContext.md) (execution context structure)
  - JsonPathItem (path item structure) 
  - [JsonbValue](../J/JsonbValue.md) (JSONB value representation)
  - [JsonValueList](../J/JsonValueList.md) (result collection structure)
  - jspAutoUnwrap (unwrapping behavior determination)
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md) (core execution implementation)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (return type enumeration)
- Called from (representative examples):
  - [executeJsonPath](executeJsonPath.md) (main execution entry point)
  - [executeNextItem](executeNextItem.md) (sequential execution)
  - [executeItemOptUnwrapResult](executeItemOptUnwrapResult.md) (result processing)
  - [getArrayIndex](../g/getArrayIndex.md) (array indexing operations)

## Notes and Other Information
- This is a thin wrapper that encapsulates the common pattern of automatic unwrapping
- Simplifies the API for callers who want standard unwrapping behavior
- Part of the recursive JSONPath execution machinery
- The unwrapping decision is context-dependent, typically based on lax vs strict execution mode
- Located in src/backend/utils/adt/jsonpath_exec.c:735-746
- Serves as an intermediate layer in the JSONPath execution hierarchy
- Essential for maintaining consistent unwrapping semantics across different execution paths

## Simplified Source

```c
static JsonPathExecResult executeItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
                                     JsonbValue *jb, JsonValueList *found) {
    // Wrapper that executes JSONPath item with automatic unwrapping
    // based on current execution context's lax mode setting
    return executeItemOptUnwrapTarget(cxt, jsp, jb, found, jspAutoUnwrap(cxt));
}
```