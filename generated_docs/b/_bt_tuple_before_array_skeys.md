# _bt_tuple_before_array_skeys

## Location
src/backend/access/nbtree/nbtutils.c: 1544 - 1667

## Overview
Determines whether it is too early to advance array keys by comparing a tuple against the current array keys in a B-tree index scan.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's B-tree array key handling mechanism. It compares a given tuple against the current set of array scan keys to determine whether the scan has reached the point where array keys need to be advanced. The function handles both forward and backward scan directions and deals with the complexity of required vs. non-required scan keys.

The function serves two main purposes:
1. For readpagetup callers: Helps determine when _bt_check_compare has set continuescan=false and whether array advancement is needed
2. For other callers: Provides detailed comparison results while optionally tracking scan positioning relative to truncated attributes

The function uses 3-way ORDER procedures for comparisons, which provides more precise results than the equality operators used by _bt_check_compare. It carefully handles inequality strategy scan keys and truncated attributes in pivot tuples.

## Parameters / Member Variables
- : Index scan descriptor containing the scan state and key information
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
currTuples
data
dataDumper
default
doc
dropped
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
slru
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
write_data_to_archive_lz4_doc.md: Scan direction (forward or backward)
- : The index tuple to compare against current array keys
- : Tuple descriptor for interpreting the tuple structure
- : Number of attributes in the tuple
- : Indicates if tuple is the scan's current _bt_readpage-wise tuple
- : Scan key trigger value from _bt_check_compare (0 for non-readpagetup callers)
- : Optional output parameter tracking whether truncated attributes affected the result

## Dependencies
- Functions called/Symbols referenced:
  - [index_getattr](../i/index_getattr.md)
  - [_bt_compare_array_skey](_bt_compare_array_skey.md)
  - ScanDirectionIsForward
  - ScanDirectionIsBackward
- Called from (representative examples):
  - [_bt_advance_array_keys](_bt_advance_array_keys.md)
  - [_bt_checkkeys](_bt_checkkeys.md)
  - [_bt_checkkeys_look_ahead](_bt_checkkeys_look_ahead.md)

## Notes and Other Information
- The function assumes that array keys are already set in scan->opaque->keyData[]
- For readpagetup callers, only one ORDER proc comparison is performed at most
- The function handles truncated attributes in high key tuples by setting *scanBehind to true
- Return value interpretation:
  - false: tuple is >= current required equality keys (time to advance arrays)
  - true: tuple is < current equality keys (not yet time to advance)
- The function is optimized to avoid redundant comparisons by using the sktrig parameter to skip already-satisfied keys