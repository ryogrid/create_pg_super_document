# plan_elem_desc

## Location
[src/backend/access/rmgrdesc/heapdesc.c:75-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/heapdesc.c#L75-L103)

## Overview
A static utility function that formats heap freeze plan elements into human-readable descriptions for WAL record debugging, specifically used to describe individual freeze plans within heap freeze operations.

## Definition
```c
static void plan_elem_desc(StringInfo buf, void *plan, void *data)
```

## Detailed Description
The `plan_elem_desc` function is a specialized formatting utility used within the heap resource manager description system to convert heap freeze plan data structures into readable string representations. It processes an individual `xlhp_freeze_plan` structure and formats its contents including transaction ID (xmax), info masks, tuple count, and associated tuple offsets.

The function is designed to work with the `array_desc` utility for handling arrays of freeze plans. It advances the offset pointer to point to the next set of offsets after processing the current plan, enabling proper sequential processing of multiple freeze plans. This function is crucial for understanding complex heap freeze operations during WAL record analysis and debugging.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted freeze plan description will be appended
- `plan`: Pointer to an `xlhp_freeze_plan` structure containing the freeze plan details
- `data`: Pointer to an array of OffsetNumber pointers, used to track current position in offset arrays

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfo
  - appendStringInfoString
  - [array_desc](../a/array_desc.md)
  - [offset_elem_desc](../o/offset_elem_desc.md)
  - [xlhp_freeze_plan](../x/xlhp_freeze_plan.md) (struct type)
  - OffsetNumber (type)
- Called from:
  - [heap2_desc](../h/heap2_desc.md)

## Notes and Other Information
- The function modifies the data pointer to advance through offset arrays, enabling correct processing of sequential freeze plans
- Formats output as "{ xmax: X, infomask: Y, infomask2: Z, ntuples: N, offsets: [...] }"
- Used specifically for describing XLOG_HEAP2_PRUNE_FREEZE records
- The freeze plan contains information about which tuples to freeze and what transaction visibility information to apply
- Part of the PostgreSQL tuple freezing mechanism which prevents transaction ID wraparound issues