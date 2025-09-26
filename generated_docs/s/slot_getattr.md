# slot_getattr

## Location
[src/include/executor/tuptable.h:395-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L395-L415)

## Overview
The primary inline function for fetching a single attribute value and its null status from a TupleTableSlot with lazy evaluation.

## Definition
```c
static inline Datum
slot_getattr(TupleTableSlot *slot, int attnum, bool *isnull)
```

## Detailed Description
This function is the main interface for retrieving individual attribute values from a TupleTableSlot. It implements lazy evaluation by only materializing the attribute if it hasn't been computed yet (when `attnum > slot->tts_nvalid`). The function efficiently handles both the attribute value and its null status in a single call.

The function first ensures that the requested attribute is materialized by calling `slot_getsomeattrs` if necessary, then returns both the Datum value and sets the null indicator through the `isnull` parameter. This is the standard way to access individual tuple attributes throughout PostgreSQL's executor.

## Parameters / Member Variables
- `slot`: The TupleTableSlot containing the tuple data
- `attnum`: The attribute number to fetch (1-based indexing)
- `isnull`: Output parameter that will be set to the attribute's null status

## Dependencies
- Functions called/Symbols referenced:
  - [slot_getsomeattrs](slot_getsomeattrs.md)
- Called from (representative examples):
  - [debugtup](../d/debugtup.md)
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md)
  - [ExecJustVarImpl](../E/ExecJustVarImpl.md)
  - [TupleHashTableHash_internal](../T/TupleHashTableHash_internal.md)
  - [FormPartitionKeyDatum](../F/FormPartitionKeyDatum.md)
  - [heap_compare_slots](../h/heap_compare_slots.md)
  - [ExecNestLoop](../E/ExecNestLoop.md)
  - [ExecScanSubPlan](../E/ExecScanSubPlan.md)
  - [buildSubPlanHash](../b/buildSubPlanHash.md)
  - [update_frameheadpos](../u/update_frameheadpos.md)
  - [ri_ExtractValues](../r/ri_ExtractValues.md)
  - [ri_KeysEqual](../r/ri_KeysEqual.md)
  - [ExecGetJunkAttribute](../E/ExecGetJunkAttribute.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance reasons
- Uses 1-based attribute numbering (attnum > 0 is asserted)
- The most commonly used function for accessing individual tuple attributes
- Efficiently combines value retrieval and null checking in a single operation
- Returns a Datum which may need type-specific casting depending on the attribute's data type
- The `isnull` parameter is an output parameter and must not be NULL
- Part of PostgreSQL's lazy evaluation system - attributes are only computed when accessed