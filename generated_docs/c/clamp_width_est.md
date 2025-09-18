# clamp_width_est

## Location
src/backend/optimizer/path/costsize.c: 231 - 253

## Overview
Forces a tuple-width estimate to a sane value by clamping it from int64 to int32 range while preventing integer overflow.

## Definition
```c
int32 clamp_width_est(int64 tuple_width)
```

## Detailed Description
This function safely converts tuple width estimates from int64 to int32 while handling potential integer overflow scenarios. The PostgreSQL planner represents datatype width and tuple width estimates as int32 values. When summing column width estimates to create a tuple width estimate, integer overflow can occur in edge cases.

To ensure numerical safety, the planner performs width calculations in int64 arithmetic and then uses this function to safely clamp the result back to the int32 range. The function ensures that no tuple width estimate exceeds MaxAllocSize, which represents the maximum amount of memory that can be allocated for a single tuple in PostgreSQL.

Unlike clamp_row_est, this function uses Assert() to verify that negative values don't occur, treating negative widths as programming errors rather than recoverable conditions.

## Parameters / Member Variables
- `tuple_width`: The input tuple width estimate in bytes (int64)

## Dependencies
- Functions called/Symbols referenced:
  - `MaxAllocSize`: PostgreSQL constant defining the maximum allocatable memory size
  - `Assert()`: PostgreSQL assertion macro for debugging

- Called from (representative examples):
  - [set_rel_width](../s/set_rel_width.md): Sets the estimated width for relation tuples
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md): Sets cost and width for path targets
  - [get_rel_data_width](../g/get_rel_data_width.md): Gets data width for relations
  - `build_joinrel_tlist`: Builds target lists for join relations

## Notes and Other Information
- Located in src/backend/optimizer/path/costsize.c:231-253
- Designed to prevent integer overflow when summing column widths
- Returns int32 values suitable for PostgreSQL's internal width representations
- Uses Assert() for negative value checking, indicating such cases should never occur
- MaxAllocSize limit ensures physical constraints are respected
- Critical for memory management in tuple processing
- Part of PostgreSQL's cost estimation infrastructure