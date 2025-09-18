# getDatumCopy

## Location
src/backend/access/gin/ginbulk.c: 128 - 147

## Overview
A specialized version of datumCopy that additionally tracks allocated memory usage in a BuildAccumulator during GIN index bulk loading.

## Definition
```c
static Datum getDatumCopy(BuildAccumulator *accum, OffsetNumber attnum, Datum value)
```

## Detailed Description
This function creates a copy of a Datum value while tracking memory allocation for bulk loading operations. It behaves similarly to the standard datumCopy function but extends it to account for allocated memory in the BuildAccumulator's allocatedMemory counter. For pass-by-value attributes, it simply returns the original value. For pass-by-reference attributes, it creates a copy using datumCopy and tracks the allocated space using GetMemoryChunkSpace.\n\n## Parameters / Member Variables\n- `accum`: Pointer to BuildAccumulator containing GIN state and memory tracking\n- `attnum`: Attribute number (1-based) to determine the attribute properties\n- `value`: The Datum value to copy\n\n## Dependencies\n- Functions called/Symbols referenced:\n  - TupleDescAttr\n  - datumCopy\n  - GetMemoryChunkSpace\n  - DatumGetPointer\n  - BuildAccumulator (struct)\n  - Form_pg_attribute (struct)\n- Called from (representative examples):\n  - ginInsertBAEntry\n\n## Notes and Other Information\n- Static function used internally within the GIN bulk loading module\n- Extends standard datumCopy functionality with memory tracking\n- Handles both pass-by-value and pass-by-reference attributes appropriately\n- Memory tracking helps monitor bulk loading memory usage\n- Part of the GIN access method's memory-aware bulk loading system