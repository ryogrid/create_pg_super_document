# JsonValueListAppend

## Location
[src/backend/utils/adt/jsonpath_exec.c:3513-3526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3513-L3526)

## Overview
Appends a JsonbValue to a JsonValueList, efficiently managing the transition from empty to singleton to multi-element list storage.

## Definition
```c
static void JsonValueListAppend(JsonValueList *jvl, JsonbValue *jbv)
```

## Detailed Description
The JsonValueListAppend function adds a JsonbValue to a JsonValueList structure using an optimized storage strategy. It handles three scenarios: (1) if the list is empty, it stores the value as a singleton; (2) if there's already a singleton value, it creates a two-element list containing both the singleton and the new value; (3) if there's already a list, it appends the new value to the existing list. This approach minimizes memory allocation for the common case of single-value results while supporting efficient multi-value operations.

## Parameters / Member Variables
- `jvl`: Pointer to the JsonValueList structure to append to
- `jbv`: Pointer to the JsonbValue to be appended to the list

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueList](JsonValueList.md) (structure type)
  - list_make2 (PostgreSQL function to create a two-element list)
  - [lappend](../l/lappend.md) (PostgreSQL function to append to a list)
- Called from (representative examples):
  - [executeNextItem](../e/executeNextItem.md) (during JSONPath item execution)
  - [executeItemOptUnwrapResult](../e/executeItemOptUnwrapResult.md) (result unwrapping operations)
  - [executeAnyItem](../e/executeAnyItem.md) (any-type item execution)

## Notes and Other Information
- Implements an optimization where single values are stored without list overhead
- Automatically transitions from singleton to list storage when a second value is added
- Uses PostgreSQL's list manipulation functions for efficient list management
- Part of the result accumulation system used throughout JSONPath execution
- The singleton optimization reduces memory overhead for the common case where JSONPath expressions yield single results
- Essential component of the JsonValueList data structure used for collecting JSONPath execution results

## Simplified Source

```c
static void JsonValueListAppend(JsonValueList *jvl, JsonbValue *jbv) {
    // Handle transition from empty -> singleton -> list
    if (jvl->singleton) {
        // Convert singleton to two-element list
        jvl->list = list_make2(jvl->singleton, jbv);
        jvl->singleton = NULL;
    }
    else if (!jvl->list) {
        // Store first value as singleton for efficiency
        jvl->singleton = jbv;
    }
    else {
        // Append to existing list
        jvl->list = lappend(jvl->list, jbv);
    }
}
```