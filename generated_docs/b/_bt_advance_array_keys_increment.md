# _bt_advance_array_keys_increment

## Location
[src/backend/access/nbtree/nbtutils.c:1381-1466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L1381-L1466)

## Overview
Advances array keys by a single increment in the current scan direction, handling rollover from lower-order arrays to higher-order arrays, and returns whether more array combinations remain.

## Definition


## Detailed Description
This function implements the core logic for advancing through combinations of array elements during B-tree index scans with multiple SK_SEARCHARRAY scan keys. It works like a multi-digit counter, advancing the lowest-order (rightmost) array first and carrying over to higher-order arrays when the current array wraps around.

The function processes arrays from right to left (highest index to lowest index), since the last array key corresponds to the lowest-order index column. For each array, it increments the current element position in the scan direction. When an array reaches its boundary (beginning or end depending on scan direction), it rolls over to the opposite boundary and continues to the next higher-order array.

If all arrays have been exhausted (all have rolled over), the function restores the arrays to their pre-call state by calling _bt_start_array_keys with the opposite direction, ensuring that arrays only advance monotonically in the scan direction. This prevents missing tuples if the scan direction is later reversed.

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
  - [_bt_start_array_keys](_bt_start_array_keys.md)
  - ScanDirectionIsForward
  - ScanDirectionIsBackward
  - BTScanOpaque (structure)
  - [BTArrayKeyInfo](../B/BTArrayKeyInfo.md) (structure)
  - [IndexScanDesc](../I/IndexScanDesc.md) (structure)
  - ScanDirection (enum)
  - ScanKey (structure)
- Called from (representative examples):
  - [_bt_advance_array_keys](_bt_advance_array_keys.md)

## Notes and Other Information
- Returns true if there are more array combinations to process, false if arrays are exhausted
- Processes arrays in reverse order (last to first) to advance lowest-order index columns first
- Implements rollover logic similar to odometer or multi-digit counter advancement
- Restores array state on exhaustion to maintain scan direction consistency
- Critical for supporting multi-column array scans in B-tree indexes
- Each scan key's sk_argument is updated to point to the new current array element
- The rollover mechanism ensures all possible array element combinations are visited exactly once