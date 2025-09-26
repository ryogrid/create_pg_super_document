# slot_is_current_xact_tuple

## Location
[src/include/executor/tuptable.h:445-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L445-L453)

## Overview
Checks if the tuple currently stored in a TupleTableSlot was created by the current transaction.

## Definition

```c
static inline bool
slot_is_current_xact_tuple(TupleTableSlot *slot)
```
## Detailed Description
This inline function provides a uniform interface for checking whether the tuple contained in a TupleTableSlot was created by the current transaction. It delegates to the slot's type-specific implementation through the tts_ops function pointer table, allowing different slot types to implement their own logic for transaction visibility checks.

The function is critical for transaction isolation and MVCC (Multi-Version Concurrency Control) operations, particularly when determining whether a tuple modification is allowed or when checking referential integrity constraints during foreign key operations.

## Parameters / Member Variables
- : Pointer to a TupleTableSlot containing the tuple to check

## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlot](../T/TupleTableSlot.md) (struct type)
  - tts_ops->is_current_xact_tuple (function pointer)
- Called from (representative examples):
  - [RI_FKey_fk_upd_check_required](../R/RI_FKey_fk_upd_check_required.md)

## Notes and Other Information
- This function requires that the slot contains a storage tuple; calling it on an empty slot or a slot type that doesn't support storage tuples will result in an error
- Callers must verify that the slot type supports storage tuples before calling this function
- The actual implementation varies by slot type through the tts_ops function table
- Primarily used in foreign key constraint checking and other referential integrity operations
- Part of the TupleTableSlot abstraction layer that provides uniform access to different tuple storage formats