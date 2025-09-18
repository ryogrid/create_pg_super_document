# spggettuple

## Location
src/backend/access/spgist/spgscan.c: 1026 - 1082

## Overview
Primary function for retrieving individual tuples during SPGiST index scans, implementing the gettuple interface for the SPGiST access method.

## Definition


## Detailed Description
This function implements the gettuple interface for SPGiST (Space-Partitioned Generalized Search Tree) indexes in PostgreSQL. It retrieves individual tuples one at a time during index scans, managing the scan state and coordinating with the SPGiST walking algorithm. The function handles both forward scanning and ORDER BY operations with distance calculations.

The function operates in a loop, first returning any previously found tuples from internal buffers, then invoking the SPGiST tree walking algorithm to find more tuples when the buffer is exhausted. It properly manages memory for distance calculations and reconstructed tuples to prevent memory leaks.

Only forward scan direction is supported, as SPGiST indexes are designed for proximity and containment searches rather than ordered traversal.

## Parameters / Member Variables
- : Index scan descriptor containing scan parameters and state
- 0
5
6
=
COPYRIGHT
ENTRY_POINTS.md
GENERATION_PLAN.md
GNUmakefile.in
GPATH
GRTAGS
GTAGS
HISTORY
I[0]
I[0],
I[1]
I[1],
I[2]
I[]
LICENSE
Makefile
README.md
README_PG.md
S[0]
__pycache__
aclocal.m4
analyze_only
assistive_info.db
bgwriter
bigint_value2
bra
bv_allnulls
c
config
configure
configure.ac
context
contrib
create_duckdb_index.py
data
dataDumper
default
doc
end_compressor_lz4_doc.md
err
extract_readme_file_header_comments.py
extract_symbol_references.py
filter_frequent_symbol_from_csv.py
filter_frequent_symbol_from_csv.py~
formatData
freeptr
global_symbols.db
global_symbols_bf_add_symbol_type.db
heap_xlog_confirm_doc.md
heap_xlog_inplace_doc.md
heap_xlog_lock_doc.md
heap_xlog_lock_updated_doc.md
heap_xlog_update_doc.md
import_symbol_reference.py
ket
l
lb
level
lower
lz4_compression_init_doc.md
lz4_stream_eof_doc.md
meson.build
meson_options.txt
nsubs
ntuples
number_of_rows
output
p
p[0]
p[0].x
p[0].y
p[1]
p[1].x
p[1].y
p[z-
pgstat_subscription_flush_cb_doc.md
prevTail
process_symbol_definitions.py
process_symbol_definitions_illegular_records.txt
public.verbose
python_version
r
read_data_from_archive_lz4_doc.md
requirements.txt
resultinfo
s
scripts
set_file_end_lines.py
src
subs
symbol_references.csv
symbol_references_filtered.csv
sys
tblspc_identify_doc.md
test
tg_event
tg_newtuple
tg_trigtuple
update_symbol_types.py
v
venv
views
write_data_to_archive_lz4_doc.md: Scan direction (only ForwardScanDirection is supported)

## Dependencies
- Functions called/Symbols referenced:
  - spgWalk
  - storeGettuple
  - index_store_float8_orderby_distances
  - elog
  - pfree
- Types used:
  - IndexScanDesc
  - ScanDirection  
  - SpGistScanOpaque
- Constants:
  - ForwardScanDirection
- Called from:
  - spghandler (as part of the access method interface)

## Notes and Other Information
- Returns true when a tuple is found, false when the scan is complete
- Only supports forward scan direction; throws an error for backward scans
- Manages internal buffers to batch tuple retrieval for efficiency
- Properly handles memory cleanup for ORDER BY distance calculations
- Integrates with PostgreSQL's index scan infrastructure
- Part of the SPGiST access method implementation
- The function maintains scan state between calls to support incremental tuple retrieval
- Coordinates with the SPGiST tree walking algorithm through spgWalk and storeGettuple