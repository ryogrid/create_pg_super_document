# _bt_verify_arrays_bt_first

## Location
[src/backend/access/nbtree/nbtutils.c:3006-3043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L3006-L3043)

## Overview
Verifies that scan's array key state is correct before starting a new primitive scan, ensuring non-required arrays start with the first element for the current scan direction.

## Definition


## Detailed Description
This function performs validation checks on the scan's array key state to ensure it's properly configured before _bt_start_prim_scan initiates a new primitive index scan. It enforces a critical rule for non-required array scan keys: they must be positioned at the first element appropriate for the current scan direction.

The function iterates through all scan keys, focusing on equality strategy array keys (SK_SEARCHARRAY with BTEqualStrategyNumber). For each non-required array key, it verifies that the current element position (cur_elem) matches the expected first element for the scan direction:
- Forward scans: first element should be at index 0
- Backward scans: first element should be at index (num_elems - 1)

Required arrays (marked with SK_BT_REQFWD or SK_BT_REQBKWD in the appropriate direction) are exempt from this check since they may legitimately be positioned elsewhere based on previous array advancement operations.

The function concludes by calling _bt_verify_keys_with_arraykeys to perform additional array key consistency checks.

## Parameters / Member Variables
- : Index scan descriptor containing array key information and scan state
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
write_data_to_archive_lz4_doc.md: Current scan direction (forward or backward)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_verify_keys_with_arraykeys](_bt_verify_keys_with_arraykeys.md)
  - ScanDirectionIsForward
  - ScanDirectionIsBackward
- Called from (representative examples):
  - [_bt_start_prim_scan](_bt_start_prim_scan.md)

## Notes and Other Information
- This is primarily a debugging/assertion function used to catch array state inconsistencies
- Only validates equality strategy array scan keys (SK_SEARCHARRAY with BTEqualStrategyNumber)  
- Required arrays are skipped because they may have been advanced by previous operations
- The rule enforced helps maintain consistency with _bt_rewind_nonrequired_arrays behavior
- Returns false if any non-required array is not positioned at the expected first element
- The validation helps ensure that new primitive scans start from a clean, predictable state
- This function is typically called within assertions to catch state corruption early