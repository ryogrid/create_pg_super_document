# _bt_start_array_keys

## Location
[src/backend/access/nbtree/nbtutils.c:1343-1380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L1343-L1380)

## Overview
Initializes array keys at the start of a B-tree index scan by setting up current element counters and filling in the first scan key argument value for each array scan key.

## Definition

```c
void
_bt_start_array_keys(IndexScanDesc scan, ScanDirection dir)
```
## Detailed Description
This function prepares array scan keys for a B-tree index scan by initializing their current element positions based on the scan direction. For forward scans, it sets the current element to the first array element (index 0), while for backward scans, it sets the current element to the last array element (num_elems - 1).

The function iterates through all array keys in the BTScanOpaque structure, setting each array key's cur_elem position and copying the corresponding element value into the scan key's sk_argument field. This ensures that when the scan begins, each array scan key is positioned at the appropriate starting element for the specified scan direction.

Additionally, the function resets the scanBehind flag to false, indicating that the scan is not currently positioned behind valid tuples.

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
update_symbol_types.py: ScanDirection indicating whether this is a forward or backward scan

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsBackward
  - BTScanOpaque (structure)
  - [BTArrayKeyInfo](../B/BTArrayKeyInfo.md) (structure)
  - [IndexScanDesc](../I/IndexScanDesc.md) (structure)
  - ScanDirection (enum)
  - ScanKey (structure)
  - SK_SEARCHARRAY (flag)
- Called from (representative examples):
  - [btrestrpos](btrestrpos.md)
  - [_bt_first](_bt_first.md)
  - [_bt_advance_array_keys_increment](_bt_advance_array_keys_increment.md)

## Notes and Other Information
- Function assumes so->numArrayKeys > 0 and so->qual_ok is true
- All array keys must have SK_SEARCHARRAY flag set and num_elems > 0
- Proper initialization is critical for correct array key advancement during scan progression
- The scanBehind flag reset ensures consistent scan state at initialization
- Forward scans start from the beginning of arrays, backward scans start from the end
- Each array scan key's sk_argument is set to point to the current array element value