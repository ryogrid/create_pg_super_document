# _hash_load_qualified_items

## Location
src/backend/access/hash/hashsearch.c: 602 - 707

## Overview
Loads all the qualified index tuples from a current hash index page into the scan's current position buffer, serving as a helper function for _hash_readpage during hash index scanning operations.

## Definition


## Detailed Description
This function is responsible for extracting all tuples from a hash index page that match the scan criteria and storing them in the scan's current position buffer. It handles both forward and backward scan directions, applying proper filtering logic to skip inappropriate tuples (such as those moved by split operations or marked as dead). The function ensures that only tuples with matching hash keys and that satisfy the scan qualifiers are loaded into the buffer.

The function implements different loading strategies based on scan direction:
- For forward scans: loads items in ascending order starting from the given offset
- For backward scans: loads items in descending order, filling the buffer from the end

## Parameters / Member Variables
- : IndexScanDesc containing the scan context and state information
- : The hash index page from which to load qualified tuples
- : Starting offset number on the page for loading tuples
- 0
1
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
cachectx
client_finished_auth
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
entry_cxt
err
extract_readme_file_header_comments.py
extract_symbol_references.py
filter_frequent_symbol_from_csv.py
filter_frequent_symbol_from_csv.py~
formatData
freeptr
gctx
global_symbols.db
global_symbols_bf_add_symbol_type.db
gssapi_used
heap_xlog_confirm_doc.md
heap_xlog_inplace_doc.md
heap_xlog_lock_doc.md
heap_xlog_lock_updated_doc.md
heap_xlog_update_doc.md
import_symbol_reference.py
ket
krbsrvname
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
r2.upper
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
usesspi
v
venv
views
write_data_to_archive_lz4_doc.md: Scan direction (forward or backward) determining the loading strategy

## Dependencies
- Functions called/Symbols referenced:
  - PageGetMaxOffsetNumber
  - ScanDirectionIsForward
  - PageGetItem
  - PageGetItemId
  - ItemIdIsDead
  - OffsetNumberNext/OffsetNumberPrev
  - _hash_get_indextuple_hashkey
  - _hash_checkqual
  - _hash_saveitem
- Called from (representative examples):
  - _hash_readpage

## Notes and Other Information
- Skips tuples marked with INDEX_MOVED_BY_SPLIT_MASK when the bucket was populated but not split during scan initialization
- Respects the ignore_killed_tuples scan flag to skip dead tuples
- Uses early termination optimization - stops loading when encountering the first non-matching tuple
- Returns the number of items loaded (itemIndex for forward scans, or adjusted itemIndex for backward scans)
- Maintains assertion checks to ensure buffer boundaries are respected (MaxIndexTuplesPerPage)