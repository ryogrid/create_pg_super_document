# json_unique_builder_get_throwawaybuf

## Location
src/backend/utils/adt/json.c: 969 - 992

## Overview
This function provides on-demand initialization of a throwaway StringInfo buffer used for reading key names that don't need to be stored in the output object during duplicate key detection.

## Definition
```c
static StringInfo json_unique_builder_get_throwawaybuf(JsonUniqueBuilderState *cxt)
```

## Detailed Description
The function manages a reusable string buffer within the JsonUniqueBuilderState context. It serves as an optimization for duplicate key detection scenarios where key names need to be read but not preserved in the final output object, particularly when the associated value is NULL. 

The function implements lazy initialization - if the buffer hasn't been created yet, it initializes a new StringInfo in the appropriate memory context. If the buffer already exists, it simply resets the length to zero, effectively reusing the allocated memory space.

## Parameters / Member Variables
- `cxt`: Pointer to JsonUniqueBuilderState containing the context and throwaway buffer state

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo
  - MemoryContextSwitchTo
- Data structures used:
  - JsonUniqueBuilderState
  - StringInfo
  - MemoryContext
- Called from (representative examples):
  - json_object_agg_transfn_worker
  - json_build_object_worker

## Notes and Other Information
- This is a static function, only accessible within the json.c compilation unit
- Implements memory-efficient string buffer reuse pattern
- The buffer is specifically used for temporary key storage during duplicate detection
- Memory context switching ensures proper allocation in the builder's memory context
- Performance optimization to avoid repeated memory allocation/deallocation cycles