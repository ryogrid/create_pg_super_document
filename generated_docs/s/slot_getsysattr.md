# slot_getsysattr

## Location
[src/include/executor/tuptable.h:416-444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L416-L444)

## Overview
An inline function that fetches system attributes (such as tableoid and ctid) from a TupleTableSlot, with built-in handling for common system attributes and delegation to slot-specific implementations.

## Definition
```c
static inline Datum
slot_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
```

## Detailed Description
This function provides access to PostgreSQL system attributes, which have negative attribute numbers and contain metadata about tuples rather than user data. The function handles two common system attributes directly: `tableoid` (TableOidAttributeNumber) and `ctid` (SelfItemPointerAttributeNumber). For these attributes, it returns values directly from the slot's cached fields (`tts_tableOid` and `tts_tid`).

For other system attributes, the function delegates to the slot type's specific `getsysattr` operation through the slot's operations table (`tts_ops->getsysattr`). This allows different slot types to provide their own implementations for system attributes that may not be directly cached in the slot structure.

## Parameters / Member Variables
- `slot`: The TupleTableSlot from which to fetch the system attribute
- `attnum`: The system attribute number to fetch (must be negative)
- `isnull`: Output parameter that will be set to the attribute's null status

## Dependencies
- Functions called/Symbols referenced:
  - TableOidAttributeNumber
  - SelfItemPointerAttributeNumber
- Called from (representative examples):
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - [execCurrentOf](../e/execCurrentOf.md)
  - [ExecEvalSysVar](../E/ExecEvalSysVar.md)
  - [ExecCheckTupleVisible](../E/ExecCheckTupleVisible.md)
  - [ExecOnConflictUpdate](../E/ExecOnConflictUpdate.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance reasons
- System attributes use negative attribute numbers (attnum < 0 is asserted)
- Only works with slot types that support system attributes - will throw an error otherwise
- Commonly accessed system attributes (tableoid, ctid) are handled with direct cached access for efficiency
- Other system attributes are delegated to slot-type-specific implementations
- The tableoid system attribute identifies which table a tuple came from
- The ctid system attribute contains the tuple's physical location (ItemPointer)
- Both directly handled attributes are never null