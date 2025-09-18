# _bt_start_prim_scan

## Location
[src/backend/access/nbtree/nbtutils.c:1668-1788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L1668-L1788)

## Overview
Determines whether to start another scheduled primitive index scan after the current one has ended, based on array key advancement status.

## Definition


## Detailed Description
This function is called after _bt_first or _bt_next return false to determine if another primitive index scan should be initiated. It is a key component of PostgreSQL's array key handling mechanism in B-tree indexes, allowing scans to continue across multiple primitive scans when dealing with equality array scan keys.

The function works by checking a flag (needPrimScan) that is set by _bt_checkkeys when array keys have been advanced and another scan is required. This design avoids redundant leaf page access by strategically advancing array keys only when it enables finding nearby matching tuples or eliminates unmatchable array key space.

The function handles various edge cases where _bt_checkkeys might not be reached (empty indexes, non-existent values beyond index boundaries) by relying on the flag-based approach rather than requiring explicit instructions for each case.

## Parameters / Member Variables
- : Index scan descriptor containing the scan state and array key information
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

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_verify_arrays_bt_first](_bt_verify_arrays_bt_first.md)
  - [_bt_parallel_done](_bt_parallel_done.md)
- Called from (representative examples):
  - [btgettuple](btgettuple.md)
  - [btgetbitmap](btgetbitmap.md)

## Notes and Other Information
- Should only be called during scans with one or more equality type array scan keys
- Must be called after _bt_first or _bt_next return false
- Resets the scanBehind flag at the start of each new primitive scan
- For parallel scans, calls _bt_parallel_done when the scan is exhausted
- The needPrimScan flag is managed by _bt_checkkeys and reset by _bt_first
- Handles edge cases gracefully where _bt_checkkeys cannot be reached due to empty indexes or boundary conditions
- Return value: true if another primitive scan should start, false if array keys are exhausted