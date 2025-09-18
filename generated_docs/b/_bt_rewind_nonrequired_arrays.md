# _bt_rewind_nonrequired_arrays

## Location
src/backend/access/nbtree/nbtutils.c: 1467 - 1543

## Overview
Resets non-required array scan keys to their first element in the current scan direction to ensure correct positioning for primitive index scans triggered by inequality operators.

## Definition


## Detailed Description
This function handles a subtle but critical aspect of B-tree array key management during primitive index scans. It specifically targets non-required arrays (those without SK_BT_REQFWD or SK_BT_REQBKWD flags) and resets them to their starting position for the current scan direction.

The function is called when _bt_advance_array_keys decides to start a new primitive index scan based on the current scan position being before a position that _bt_first can handle with inequality operators. This scenario arises in complex queries with multiple array conditions and inequality constraints.

The key insight is that while equality strategy scan keys are typically marked as required in both directions or neither, non-required arrays still participate in scan positioning during _bt_first operations. For example, in a query like "WHERE a IN (100, 200) AND b >= 3 AND c IN (5, 6, 7)", the array on column 'c' may be non-required but still influences the initial scan positioning.

To maintain correctness, when starting a new primitive scan, all non-required arrays must be reset to their first element (index 0 for forward scans, num_elems-1 for backward scans) to prevent _bt_first from using stale array positions that could lead to incorrect scan positioning.

## Parameters / Member Variables
- : IndexScanDesc containing the index scan descriptor with array key information
- 0
5
=
ENTRY_POINTS.md
FuzzyAttrMatchState_documentation.md
GENERATION_PLAN.md
LICENSE
Pfdebug
R
README.md
T2
W
__pycache__
area
attnums
base.nKeys
baserestrictcost
blockState
canon_pathkeys
contrib
create_duckdb_index.py
curTransactionContext
curaggcontext
data
ec_merging_done
es_query_cxt
estimate
extract_readme_file_header_comments.py
extract_symbol_references.py
filter_frequent_symbol_from_csv.py
framehead_slot
frameheadpos
frametail_slot
frametailpos
glob-
global_symbols.db
import_symbol_reference.py
inh
initial_rels
log
lsn[]
maxParallelHazard
output
p_next_resno
parent
parent_relid
process_symbol_definitions.py
process_symbol_definitions_illegular_records.txt
processed_tlist
python_version
requirements.txt
ri_ChildToRootMap
ri_ReturningSlot
ri_TrigNewSlot
ri_TrigOldSlot
rows
rs_ctup.t_data
scripts
set_file_end_lines.py
src
state
symbol_references.csv
symbol_references_filtered.csv
syncrep_method
temp_slot_2
type
update_colnos
update_symbol_types.py: ScanDirection indicating the current scan direction (forward or backward)

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - BTScanOpaque (structure)
  - [BTArrayKeyInfo](../B/BTArrayKeyInfo.md) (structure)
  - [IndexScanDesc](../I/IndexScanDesc.md) (structure)
  - ScanDirection (enum)
  - ScanKey (structure)
  - SK_SEARCHARRAY (flag)
  - SK_BT_REQFWD (flag)
  - SK_BT_REQBKWD (flag)
- Called from (representative examples):
  - [_bt_advance_array_keys](_bt_advance_array_keys.md)

## Notes and Other Information
- Only affects non-required arrays (those without SK_BT_REQFWD or SK_BT_REQBKWD flags)
- Required for correctness in complex queries mixing array conditions with inequalities  
- Prevents _bt_first from using incorrect array positions during primitive index scans
- Works with _bt_verify_arrays_bt_first assertions to ensure correctness
- Resets arrays to first element: index 0 for forward scans, num_elems-1 for backward scans
- Only processes SK_SEARCHARRAY scan keys with BTEqualStrategyNumber strategy
- Critical for maintaining scan correctness when transitioning between different primitive scans