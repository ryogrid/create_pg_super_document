# slot_modify_data

## Location
src/backend/replication/logical/worker.c: 900 - 992

## Overview
Creates a modified tuple by copying data from a source tuple slot and selectively replacing specified columns with new values from logical replication data, similar to heap_modify_tuple but with type conversion handling.

## Definition
```c
static void
slot_modify_data(TupleTableSlot *slot, TupleTableSlot *srcslot,
                 LogicalRepRelMapEntry *rel,
                 LogicalRepTupleData *tupleData)
```

## Detailed Description
This function implements a selective tuple modification mechanism for logical replication UPDATE operations. It creates a new tuple by starting with a complete copy of an existing tuple (from srcslot) and then selectively replacing only the columns that have changed according to the replication data.

The function operates in several phases:
1. Clears the destination slot to prepare for virtual tuple creation
2. Copies all attribute values and null flags from the source slot
3. Iterates through the attribute mapping to identify columns with changes
4. For each changed column, converts the new data from replication format to internal format
5. Handles both text and binary data formats using appropriate type conversion functions
6. Preserves unchanged columns with their original values from the source slot

The function is designed to be memory-efficient by only converting and replacing columns that have actually changed, while preserving the original values for unchanged columns. However, it includes a caution that unreplaced pass-by-reference columns will point into the storage of the source slot.

## Parameters / Member Variables
- `slot`: TupleTableSlot where the modified tuple will be stored (destination)
- `srcslot`: TupleTableSlot containing the original tuple data to be copied and modified (source)
- `rel`: LogicalRepRelMapEntry containing the mapping between local and remote relation attributes
- `tupleData`: LogicalRepTupleData containing the new values for columns that should be updated

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple
  - slot_getallattrs
  - TupleDescAttr
  - getTypeInputInfo
  - OidInputFunctionCall
  - getTypeBinaryInputInfo
  - OidReceiveFunctionCall
  - ExecStoreVirtualTuple
  - memcpy (for bulk copying of attribute arrays)
- Called from (representative examples):
  - apply_handle_update_internal
  - apply_handle_tuple_routing

## Notes and Other Information
- This is a static function used internally within the logical replication worker
- Designed specifically for UPDATE operations where only some columns need to be changed
- Supports both LOGICALREP_COLUMN_TEXT and LOGICALREP_COLUMN_BINARY data formats
- Handles LOGICALREP_COLUMN_UNCHANGED by preserving original values from srcslot
- Treats LOGICALREP_COLUMN_NULL as an explicit NULL value assignment
- Uses memcpy for efficient bulk copying of datum and null arrays
- Includes cursor management for binary data to support re-parsing scenarios
- Validates complete consumption of binary data, reporting errors for incomplete parsing
- Uses apply_error_callback_arg for enhanced error reporting during type conversion
- Memory sharing caveat: pass-by-reference values in the result slot may reference srcslot storage
- The function assumes that both source and destination slots have the same tuple descriptor structure
- More efficient than creating a completely new tuple when only some columns have changed
- Critical for logical replication UPDATE processing where minimizing data conversion overhead is important