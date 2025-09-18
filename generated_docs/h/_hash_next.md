# _hash_next

## Location
src/backend/access/hash/hashsearch.c: 48 - 130

## Overview
Advances a hash index scan to the next item, handling pagination across overflow pages and managing scan direction (forward/backward).

## Definition


## Detailed Description
This function implements the core scanning logic for hash indexes by advancing to the next tuple in the specified direction. It manages the current scan position within a page and handles transitions between pages when the current page is exhausted. The function maintains proper buffer management including killing dead items before moving to new pages, and handles both forward and backward scan directions with appropriate overflow page navigation.

The function operates on the current scan position (so->currPos) which tracks the current page, item index, and navigation information. When advancing beyond the current page boundaries, it fetches the next/previous page and reads its contents. The scan maintains pins on bucket pages throughout the operation for efficiency.

## Parameters / Member Variables
- : IndexScanDesc containing the scan state and relation information
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
write_data_to_archive_lz4_doc.md: ScanDirection indicating whether to scan forward or backward through the index

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - [_hash_kill_items](_hash_kill_items.md)  
  - BlockNumberIsValid
  - [_hash_getbuf](_hash_getbuf.md)
  - [_hash_readpage](_hash_readpage.md)
  - [_hash_dropbuf](_hash_dropbuf.md)
  - [_hash_dropscanbuf](_hash_dropscanbuf.md)
  - HashScanPosInvalidate
- Called from (representative examples):
  - [hashgettuple](hashgettuple.md)
  - [hashgetbitmap](hashgetbitmap.md)

## Notes and Other Information
The function ensures proper cleanup of killed items before page transitions and maintains buffer pins appropriately. For backward scans, special handling is required for bucket page pins to avoid double-pinning. The function returns false when the scan is exhausted and true when a valid next tuple is found, setting scan->xs_heaptid to the result tuple's TID.