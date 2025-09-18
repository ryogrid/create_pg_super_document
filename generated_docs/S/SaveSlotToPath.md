# SaveSlotToPath

## Location
src/backend/replication/slot.c: 2014 - 2168

## Overview
Atomically saves a replication slot's persistent data to disk with checksumming, proper synchronization, and crash safety guarantees.

## Definition


## Detailed Description
This function implements the core persistence mechanism for replication slots, handling the atomic write of slot data to disk. The operation follows a carefully designed sequence to ensure crash safety and data integrity:

1. **Dirty Check**: Verifies if the slot actually needs saving by checking the dirty flag
2. **Locking**: Acquires io_in_progress_lock to prevent concurrent I/O operations
3. **Atomic Write**: Creates a temporary state file, writes slot data with checksums, and atomically renames it
4. **Synchronization**: Performs fsync operations on the file and directories to ensure durability
5. **State Update**: Updates the slot's dirty state and tracks the last saved confirmed_flush LSN

The function uses checksums to detect corruption, wait events for monitoring, and critical sections for operations that must complete atomically or trigger server restart.

## Parameters
- : Pointer to the ReplicationSlot structure containing data to be persisted
- 0
5
=
ENTRY_POINTS.md
FuzzyAttrMatchState_documentation.md
GENERATION_PLAN.md
LICENSE
MAX_SIMPLE_CHR
Pfdebug
R
README.md
SharedPromoteIsTriggered
T2
W
__pycache__
agginfos
append_rel_list
area
attnums
auth
base.nKeys
baserestrictcost
blockState
canon_pathkeys
check_agg_arguments_context_documentation.md
contrib
create_duckdb_index.py
curTransactionContext
curaggcontext
d.arraycoerce.amstate
d.arraycoerce.elemexprstate
d.arraycoerce.resultelemtype
data
dest_dboid
ec_merging_done
enc
es_query_cxt
estimate
extract_readme_file_header_comments.py
extract_symbol_references.py
filter_frequent_symbol_from_csv.py
framehead_slot
frameheadpos
frametail_slot
frametailpos
functions
glob-
global_symbols.db
gss
gss-
ii_CheckedUnchanged
import_symbol_reference.py
inh
initial_rels
join_cur_level
join_rel_level[join_cur_level]
join_rel_level[level]
lastPHId
lastnopr
lastpost
line_buf
log
lsn[]
maxParallelHazard
nentries
nwords
outbufsize
output
p_next_resno
parallelModeNeeded
parent
parent_relid
parse
pending_srf_tuples
printed_subplans
process_symbol_definitions.py
process_symbol_definitions_illegular_records.txt
processed_tlist
python_version
query_pathkeys
raw_fields[]
recoveryWakeupLatch
remoterel.attnames[i]
remoterel.natts
requirements.txt
resnull
resvalue
ri_ChildToRootMap
ri_ReturningSlot
ri_TrigNewSlot
ri_TrigOldSlot
rows
rs_ctup.t_data
scripts
search
set_file_end_lines.py
setop_pathkeys
src
src_dboid
state
strategy
symbol_references.csv
symbol_references_filtered.csv
syncrep_method
temp_slot_2
type
update_colnos
update_symbol_types.py: Directory path where the slot state should be saved
- : Error reporting level (ERROR, LOG, etc.) for handling failures

## Dependencies
- Functions called/Symbols referenced:
  -  (for slot->mutex)
  -  (for slot->io_in_progress_lock)
  - , , 
  - , 
  - , , 
  - , ,  (checksum operations)
  -  (wait event reporting)
  -  (directory synchronization)
  -  (critical section protection)
- Called from:
  -  (src/backend/replication/slot.c:999)
  -  (src/backend/replication/slot.c:1882)
  -  (src/backend/replication/slot.c:1988)

## Notes and Other Information
- This is a static function used internally within the slot.c file
- Implements atomic write-rename pattern to prevent partial state corruption
- Uses checksums (CRC32C) to detect data corruption during reads
- Reports wait events (WAIT_EVENT_REPLICATION_SLOT_WRITE/SYNC) for monitoring
- Critical sections ensure server restart if final fsync operations fail
- Handles the just_dirtied flag to track concurrent modifications during save
- Preserves errno values across lock operations for accurate error reporting
- Tracks last_saved_confirmed_flush to optimize future checkpoint decisions
- Uses O_EXCL flag to ensure temporary files don't already exist