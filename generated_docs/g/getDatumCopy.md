# getDatumCopy

## Location
[src/backend/access/gin/ginbulk.c:128-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbulk.c#L128-L147)

## Overview
A specialized version of datumCopy that additionally tracks allocated memory usage in a BuildAccumulator during GIN index bulk loading.

## Definition
```c
static Datum getDatumCopy(BuildAccumulator *accum, OffsetNumber attnum, Datum value)
```

## Detailed Description
This function creates a copy of a Datum value while tracking memory allocation for bulk loading operations. It behaves similarly to the standard datumCopy function but extends it to account for allocated memory in the BuildAccumulator's allocatedMemory counter. For pass-by-value attributes, it simply returns the original value. For pass-by-reference attributes, it creates a copy using datumCopy and tracks the allocated space using GetMemoryChunkSpace.

## Parameters / Member Variables
- `accum`: Pointer to BuildAccumulator containing GIN state and memory tracking
- `attnum`: Attribute number (1-based) to determine the attribute properties
- `value`: The Datum value to copy

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - [datumCopy](../d/datumCopy.md)
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [BuildAccumulator](../B/BuildAccumulator.md) (struct)
  - Form_pg_attribute (struct)
- Called from (representative examples):
  - [ginInsertBAEntry](ginInsertBAEntry.md)

## Notes and Other Information
- Static function used internally within the GIN bulk loading module
- Extends standard datumCopy functionality with memory tracking
- Handles both pass-by-value and pass-by-reference attributes appropriately
- Memory tracking helps monitor bulk loading memory usage
- Part of the GIN access method's memory-aware bulk loading system

## Simplified Source

```c
static Datum getDatumCopy(BuildAccumulator *accum, OffsetNumber attnum, Datum value) {
    // Get attribute information for the specified column
    Form_pg_attribute attr = TupleDescAttr(accum->ginstate->origTupdesc, attnum - 1);

    if (attr->attbyval) {
        // Pass-by-value: no copy needed, just return original value
        return value;
    } else {
        // Pass-by-reference: make a copy and track memory usage
        Datum copy = datumCopy(value, false, attr->attlen);
        accum->allocatedMemory += GetMemoryChunkSpace(DatumGetPointer(copy));
        return copy;
    }
}
```