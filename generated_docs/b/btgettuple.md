# btgettuple

## Location
[src/backend/access/nbtree/nbtree.c:206-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L206-L265)

## Overview
The btgettuple function retrieves the next tuple in a B-tree index scan, handling scan initialization, direction changes, and tuple killing optimization.

## Definition

```c
bool
btgettuple(IndexScanDesc scan, ScanDirection dir)
```
## Detailed Description
The btgettuple function is the main interface for sequential tuple retrieval during B-tree index scans. It manages the scan state and coordinates with lower-level B-tree scanning functions to return tuples in the requested order. The function handles both initial scan setup (by calling _bt_first for the first tuple) and subsequent tuple retrieval (by calling _bt_next for following tuples).

A key optimization handled by this function is "tuple killing" - marking tuples as dead when they are no longer needed, which helps with vacuum operations. The function also supports array key scanning, where multiple primitive scans may be needed to satisfy complex scan conditions. B-tree scans are never lossy, meaning all returned tuples are guaranteed to satisfy the scan conditions without requiring rechecking.

## Parameters / Member Variables
- : IndexScanDesc containing the scan state and parameters
COPYRIGHT
ENTRY_POINTS.md
GENERATION_PLAN.md
GNUmakefile.in
GPATH
GRTAGS
GTAGS
HISTORY
Makefile
README.md
README_PG.md
__pycache__
aclocal.m4
assistive_info.db
bv_allnulls
config
configure
configure.ac
contrib
create_duckdb_index.py
data
default
doc
extract_readme_file_header_comments.py
extract_symbol_references.py
filter_frequent_symbol_from_csv.py
filter_frequent_symbol_from_csv.py~
global_symbols.db
global_symbols_bf_add_symbol_type.db
import_symbol_reference.py
meson.build
meson_options.txt
ntuples
output
prevTail
process_symbol_definitions.py
process_symbol_definitions_illegular_records.txt
python_version
requirements.txt
scripts
set_file_end_lines.py
src
symbol_references.csv
symbol_references_filtered.csv
sys
test
update_symbol_types.py
venv
views: ScanDirection indicating whether to scan forward or backward

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_first](_bt_first.md) (initializes scan and gets first tuple)
  - [_bt_next](_bt_next.md) (advances scan to next tuple)
  - [_bt_start_prim_scan](_bt_start_prim_scan.md) (starts new primitive scan for array keys)
  - BTScanPosIsValid (checks if scan position is valid)
  - BTScanOpaque, IndexScanDesc, ScanDirection (type definitions)
  - MaxTIDsPerBTreePage (constant for killed items array sizing)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - [bthandler](bthandler.md) (registered as amgettuple callback)
  - Index scan execution in query processing

## Notes and Other Information
- Returns boolean indicating whether a tuple was found
- Sets scan->xs_recheck to false since B-tree scans are never lossy
- Implements tuple killing optimization to improve vacuum performance
- Supports array key scans through primitive scan iteration
- Handles scan direction changes gracefully
- Memory for killed items array is allocated on-demand
- The killed items array prevents memory overruns when scan direction is reversed
- Part of PostgreSQL's standard index access method interface for tuple-at-a-time retrieval