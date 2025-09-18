# cost_material

## Location
src/backend/optimizer/path/costsize.c: 2453 - 2508

## Overview
Calculates the startup and total costs for materializing a relation, including the overhead of reading input data and potential disk spilling when data exceeds work_mem.

## Definition
void cost_material(Path *path, Cost input_startup_cost, Cost input_total_cost, double tuples, int width)

## Detailed Description
The cost_material function estimates the cost of executing a Material node in PostgreSQL's query planner. Materialization involves storing the complete result set of an input relation so it can be accessed multiple times, which is useful for nested loop joins where the inner relation needs to be scanned repeatedly.

The costing model considers two scenarios:
1. In-memory materialization when data fits within work_mem
2. Disk-based materialization when data exceeds work_mem, requiring sequential I/O operations

The function charges a bookkeeping overhead of 2x cpu_operator_cost per tuple, which is intentionally higher than the rescan cost (cpu_operator_cost per tuple) to ensure the planner prefers materializing smaller relations. This cost reflects the overhead of managing the materialized data without qual-checking or projection operations.

## Parameters / Member Variables
- : The Path node to store the calculated costs in
- : Startup cost from the input relation
- : Total cost from the input relation  
- : Number of tuples to be materialized
- : Average width in bytes of each tuple

## Dependencies
- Functions called/Symbols referenced:
  - relation_byte_size (calculates total bytes for given tuples and width)
  - Cost (cost data type)
  - BLCKSZ (block size constant)
- Called from (representative examples):
  - create_material_path (in pathnode.c:1584)
  - materialize_finished_plan (in createplan.c:6553)

## Notes and Other Information
- Estimates costs for the first scan only; rescan savings are calculated separately in cost_rescan
- Uses work_mem threshold to determine if disk spilling is required
- Charges seq_page_cost per page when spilling to disk
- Bookkeeping overhead (2x cpu_operator_cost) is higher than rescan cost to prefer materializing smaller relations
- Assumes spilling costs are evenly distributed during execution, though this may not be perfectly accurate
- Material nodes have lower overhead than most plan nodes since they don't perform qual-checking or projection