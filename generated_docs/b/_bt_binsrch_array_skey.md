# _bt_binsrch_array_skey

## Location
[src/backend/access/nbtree/nbtutils.c:1201-1342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L1201-L1342)

## Overview
Performs binary search within SK_SEARCHARRAY arrays to find the first array element greater than or equal to a given tuple datum, with optimizations for required arrays that advance in lockstep with index scans.

## Definition

```c
static int
_bt_binsrch_array_skey(FmgrInfo *orderproc,
					   bool cur_elem_trig, ScanDirection dir,
					   Datum tupdatum, bool tupnull,
					   BTArrayKeyInfo *array, ScanKey cur,
					   int32 *set_elem_result)
```
## Detailed Description
This function implements an optimized binary search algorithm for finding the next matching array element in PostgreSQL's B-tree index array operations. It returns an index to the first array element that is greater than or equal to the caller's tupdatum argument, following a convention that favors forward scan operations.

The function includes intelligent optimizations for required arrays (those with SK_BT_REQFWD flag) that advance in lockstep with index scans. When cur_elem_trig is true, indicating that array advancement was triggered by this array's scan key, the function can dramatically reduce the number of comparisons needed by leveraging the fact that elements up to and including the former-current element can be safely excluded from the search.

The algorithm also performs optimistic comparisons for elements immediately adjacent to the former-current element, as these are often exact matches or close near-matches in the key space.

## Parameters / Member Variables
- : FmgrInfo pointer to the ordering procedure function for comparisons
- : Boolean indicating if array advancement was triggered by this array's scan key
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
update_symbol_types.py: ScanDirection specifying the current scan direction (forward/backward)
- : The datum value being searched for
- : Boolean indicating whether tupdatum is NULL
- : BTArrayKeyInfo structure containing the array elements and current position
- : ScanKey containing strategy and flag information
- : Output parameter set to the comparison result between found element and tupdatum

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_compare_array_skey](_bt_compare_array_skey.md)
  - ScanDirectionIsNoMovement
  - ScanDirectionIsForward
  - [BTArrayKeyInfo](../B/BTArrayKeyInfo.md) (structure)
  - ScanDirection (enum)
  - SK_SEARCHARRAY (flag)
  - SK_BT_REQFWD (flag)
- Called from (representative examples):
  - [_bt_compare_array_scankey_args](_bt_compare_array_scankey_args.md)
  - [_bt_advance_array_keys](_bt_advance_array_keys.md)

## Notes and Other Information
- The function assumes scan key strategy is BTEqualStrategyNumber and SK_SEARCHARRAY flag is set
- Optimizations for required arrays (SK_BT_REQFWD) can significantly reduce comparison overhead
- The algorithm is designed to work efficiently for both forward and backward scans
- Returns the low_elem index even when no exact match is found, allowing callers to handle beyond-end conditions
- Early termination occurs when an exact match is found during binary search
- The optimistic comparison technique leverages spatial locality in B-tree key space