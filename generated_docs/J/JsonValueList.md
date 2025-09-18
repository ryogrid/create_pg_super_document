# JsonValueList

## Location
[src/backend/utils/adt/jsonpath_exec.c:149-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L149-L153)

## Overview
A list structure for holding JSONB values with an optimization for single-value lists, used throughout PostgreSQL's JSON path execution engine.

## Definition
```c
typedef struct JsonValueList
{
    JsonbValue *singleton;
    List       *list;
} JsonValueList;
```

## Detailed Description
JsonValueList provides an efficient container for managing collections of JsonbValue objects during JSON path expression evaluation. It implements a space-saving optimization where single-value lists are stored directly in the singleton field rather than creating a full List structure. This design reduces memory overhead and improves performance for the common case of single-result operations while still supporting multi-value collections when needed.

## Parameters / Member Variables
- `singleton`: Direct pointer to a single JsonbValue for single-item lists (optimization)
- `list`: Standard PostgreSQL List structure for multi-item collections

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbValue](JsonbValue.md)
  - [List](../L/List.md) (PostgreSQL list type)
- Called from (representative examples):
  - [executeJsonPath](../e/executeJsonPath.md)
  - [executeItem](../e/executeItem.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [JsonValueListClear](JsonValueListClear.md)
  - [JsonValueListAppend](JsonValueListAppend.md)
  - [JsonValueListLength](JsonValueListLength.md)
  - [JsonValueListIsEmpty](JsonValueListIsEmpty.md)

## Notes and Other Information
- Uses a singleton optimization to avoid List overhead for single values
- Extensively used throughout the JSON path execution engine
- Has associated utility functions like JsonValueListClear, JsonValueListAppend, etc.
- The choice between singleton and list storage is transparent to most callers
- Critical for efficient JSON path result collection and processing