# slot_getsomeattrs_int

## Location
[src/backend/executor/execTuples.c:1989-2024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1989-L2024)

## Overview
Internal workhorse function that ensures a TupleTableSlot has valid values for all attributes up to a specified attribute number, filling in missing attributes as needed.

## Definition
```c
void slot_getsomeattrs_int(TupleTableSlot *slot, int attnum)
```

## Detailed Description
This function serves as the internal implementation for slot_getsomeattrs(), ensuring that all attribute values up to the specified attribute number are valid and accessible in the slot. It operates in two phases:

1. **Fetch from underlying tuple**: Uses the slot's operation vector to fetch as many attributes as possible from the underlying tuple storage
2. **Fill missing attributes**: If the underlying tuple doesn't contain enough attributes (due to schema evolution), it calls slot_getmissingattrs to fill in the remaining attributes with their default or NULL values

The function includes validation to ensure the requested attribute number is valid and updates the slot's tts_nvalid counter to reflect the number of valid attributes after the operation completes.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to populate with attribute values
- `attnum`: The target attribute number up to which all attributes should be valid (1-based)

## Dependencies
- Functions called/Symbols referenced:
  - [slot_getmissingattrs](slot_getmissingattrs.md) (for filling missing attributes)
  - slot->tts_ops->getsomeattrs (virtual function call to slot-specific implementation)
  - Assert, elog, unlikely (PostgreSQL utility macros/functions)
- Called from (representative examples):
  - [slot_getsomeattrs](slot_getsomeattrs.md) (inline wrapper function)
  - JIT compiled code paths

## Notes and Other Information
- This is an internal function that should not be called directly; use slot_getsomeattrs() instead
- The function assumes slot->tts_nvalid < attnum (verified by assertion)
- Uses PostgreSQL's unlikely() macro to optimize the common case where missing attributes are not needed
- The function handles schema evolution scenarios where newer code expects more attributes than older tuple formats contain
- Updates slot->tts_nvalid to maintain consistency after attribute fetching
- Error handling includes validation that attnum doesn't exceed the tuple descriptor's attribute count

## Simplified Source

```c
void slot_getsomeattrs_int(TupleTableSlot *slot, int attnum) {
    // Validate input parameters
    Assert(slot->tts_nvalid < attnum);
    Assert(attnum > 0);

    if (unlikely(attnum > slot->tts_tupleDescriptor->natts)) {
        elog(ERROR, "invalid attribute number %d", attnum);
    }

    // Fetch attributes from the underlying tuple using slot-specific method
    slot->tts_ops->getsomeattrs(slot, attnum);

    // Fill in missing attributes if tuple doesn't have enough
    if (unlikely(slot->tts_nvalid < attnum)) {
        slot_getmissingattrs(slot, slot->tts_nvalid, attnum);
        slot->tts_nvalid = attnum;
    }
}
```