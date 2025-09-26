# slot_getallattrs

## Location
[src/include/executor/tuptable.h:368-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L368-L380)

## Overview
A convenience inline function that forces all attributes of a TupleTableSlot to be materialized, making them directly accessible via the slot's Datum/isnull arrays.

## Definition
```c
static inline void
slot_getallattrs(TupleTableSlot *slot)
```

## Detailed Description
This function ensures that all entries of the slot's Datum/isnull arrays are valid by calling `slot_getsomeattrs` with the total number of attributes in the tuple descriptor. After calling this function, callers can directly extract data from the slot's tts_values and tts_isnull arrays instead of using the slower `slot_getattr` function for individual attribute access.

This is particularly useful when multiple attributes need to be accessed, as it avoids the overhead of repeated validity checks and provides direct array access to all attribute values.

## Parameters / Member Variables
- `slot`: The TupleTableSlot whose all attributes should be materialized

## Dependencies
- Functions called/Symbols referenced:
  - slot_getsomeattrs
- Called from (representative examples):
  - printsimple
  - printtup
  - execute_attr_map_slot
  - DoCopyTo
  - CopyOneRowTo
  - ATRewriteTable
  - ExecEvalWholeRowVar
  - ExecFilterJunk
  - tuples_equal
  - agg_retrieve_hash_table_in_memory
  - MemoizeHash_equal
  - ExecComputeStoredGenerated
  - logicalrep_write_tuple

## Notes and Other Information
- This is an inline function defined in the header file for performance reasons
- Particularly efficient when all or most attributes of a tuple will be accessed
- After calling this function, direct array access via slot->tts_values[attno-1] and slot->tts_isnull[attno-1] is safe
- Commonly used in operations that need to process entire tuples such as COPY, tuple comparison, and tuple serialization
- The function uses the tuple descriptor's natts field to determine how many attributes to materialize