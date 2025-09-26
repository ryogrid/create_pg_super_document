# get_stats_slot_range

## Location
[src/backend/utils/adt/selfuncs.c:6090-6152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6090-L6152)

## Overview
Scans a statistics slot to find minimum and maximum values among the stored values, used as a subroutine for get_variable_range to update min/max/have_data according to statistics array contents.

## Definition

```c
static void
get_stats_slot_range(AttStatsSlot *sslot, Oid opfuncoid, FmgrInfo *opproc,
					 Oid collation, int16 typLen, bool typByVal,
					 Datum *min, Datum *max, bool *p_have_data)
```
## Detailed Description
This function examines all values in a statistics slot (AttStatsSlot) to determine the minimum and maximum values using a specified comparison operator. It iterates through the slot's values array, comparing each value against the current min/max using the provided comparison function. The function handles the initial case when no data has been processed yet by setting both min and max to the first encountered value. When new extreme values are found, they are copied using datumCopy to ensure proper memory management.

## Parameters / Member Variables
- : AttStatsSlot containing the statistics values to scan
- : OID of the comparison function to use for ordering values
- : FmgrInfo structure for the comparison function (cached for efficiency)
- : Collation to use when calling the comparison function
- : Length of the data type (-1 for variable length types)
- : Whether the data type is passed by value or reference
- : Pointer to current minimum value, updated if a smaller value is found
- : Pointer to current maximum value, updated if a larger value is found
- : Pointer to boolean indicating whether any data has been processed

## Dependencies
- Functions called/Symbols referenced:
  - [AttStatsSlot](../A/AttStatsSlot.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [get_variable_range](get_variable_range.md)

## Notes and Other Information
This is a static helper function specifically designed for range estimation in PostgreSQL's query planner. It uses the function manager (fmgr) system to call comparison operators dynamically, allowing it to work with any orderable data type. The function optimizes by caching the comparison function in the FmgrInfo structure to avoid repeated lookups. Memory management is handled carefully by copying found extreme values to prevent issues with temporary or freed memory.