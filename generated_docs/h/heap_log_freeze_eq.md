# heap_log_freeze_eq

## Location
[src/backend/access/heap/pruneheap.c:1896-1911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1896-L1911)

## Overview
Compares a freeze plan with a HeapTupleFreeze structure to determine if they represent equivalent freeze operations for tuple consolidation in WAL logging.

## Definition


## Detailed Description
This inline function performs a field-by-field comparison between an existing freeze plan (xlhp_freeze_plan) and a new HeapTupleFreeze request to determine if they describe identical freeze operations. The comparison is used to optimize WAL logging by consolidating multiple equivalent freeze operations into a single plan.

The function checks four critical fields that define the freeze operation: the maximum transaction ID (xmax), the tuple's information mask bits (t_infomask and t_infomask2), and the freeze flags (frzflags). If all these fields match exactly, the operations are considered equivalent and can be grouped together for more efficient logging.

This optimization is particularly important during vacuum operations where many tuples might require similar freeze operations, allowing the system to batch similar operations and reduce WAL volume.

## Parameters / Member Variables
- : Pointer to the existing freeze plan to compare against
- : Pointer to the HeapTupleFreeze structure containing the new freeze request

## Dependencies
- Functions called/Symbols referenced:
  - [xlhp_freeze_plan](../x/xlhp_freeze_plan.md) (struct type)
  - [HeapTupleFreeze](../H/HeapTupleFreeze.md) (struct type)
- Called from (representative examples):
  - [heap_log_freeze_plan](heap_log_freeze_plan.md)

## Notes and Other Information
- This is a static inline function for optimal performance during vacuum operations
- Used for WAL logging optimization by consolidating equivalent freeze operations
- If operations don't match, the caller must call heap_log_freeze_new_plan to create a new plan
- Critical for reducing WAL volume during vacuum operations with many similar freeze requests
- The comparison is exact - all four fields must match precisely for plans to be considered equivalent