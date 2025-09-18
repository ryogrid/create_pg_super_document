# _bt_check_compare

## Location
src/backend/access/nbtree/nbtutils.c: 3682 - 3887

## Overview
Tests whether an index tuple satisfies the current scan condition, determining if the tuple matches scan keys and setting continuation flags for scan optimization.

## Definition


## Detailed Description
This function is a core subroutine of B-tree index scanning that evaluates whether an index tuple satisfies the scan conditions. It iterates through scan keys, checking each one against the corresponding tuple attributes. The function handles various key types including NULL tests, row-comparison keys, and array keys. It optimizes scan performance by setting the  flag to false when it determines that no future tuples can satisfy required conditions, allowing the scan to terminate early. For array keys, it can advance non-required arrays to find matching values. The function is designed to work with both forward and backward scans, handling direction-specific requirements appropriately.

## Parameters / Member Variables
- : Index scan descriptor containing scan state and keys
- 0
1
3.2
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
L_currency_symbol
L_negative_sign
L_positive_sign
L_thousands_sep
Makefile
Num-
README.md
README_PG.md
S[0]
WriteDataPtr
__pycache__
aclocal.m4
analyze_only
array1
assistive_info.db
b
bgwriter
bigint_value2
bos[a-
bra
bv_allnulls
c
cachectx
client_finished_auth
co]
config
configure
configure.ac
context
contrib
copy_already_done
create_duckdb_index.py
cs
currToc
currTuples
cursor
curwords
data
dataDumper
dataOnly
decimal
default
doc
dropped
dump
end_compressor_lz4_doc.md
entry_cxt
eos[a-
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
indicator
inout_p
ins
ket
krbsrvname
l
last_relevant
lb
len
level
looids[]
lower
lz4_compression_init_doc.md
lz4_stream_eof_doc.md
member
meson.build
meson_options.txt
need_locale
next
nodeEqual
nsubs
ntuples
num_curr
num_in
number
number_of_rows
number_p
numlos
nwrds
oneCol
output
p
p[0]
p[0].x
p[0].y
p[1]
p[1].x
p[1].y
p[z-
permutations
pgstat_subscription_flush_cb_doc.md
prev-
prevTail
process_symbol_definitions.py
process_symbol_definitions_illegular_records.txt
public.std_strings
public.verbose
python_version
r
r2.upper
range2
read_data_from_archive_lz4_doc.md
read_dec
read_post
read_pre
requirements.txt
reslen
resultinfo
s
scripts
set_file_end_lines.py
sign
sign_wrote
slru
src
strict_names
subs
symbol_references.csv
symbol_references_filtered.csv
sys
tblspc_identify_doc.md
test
tg_event
tg_newtuple
tg_trigtuple
tmp
typescript
update_symbol_types.py
usesspi
v
variable
venv
views
wrds
writeData
write_data_to_archive_lz4_doc.md: Scan direction (forward or backward)
- : Index tuple being tested
- : Number of attributes in the tuple
- : Tuple descriptor defining attribute types
- : Whether to advance non-required array keys
- : Whether some keys are known to be satisfied
- : Whether this is the first matching tuple on the page
- : Output flag indicating if scan should continue
- : Input/output key index being processed

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_check_rowcompare](_bt_check_rowcompare.md)
  - [index_getattr](../i/index_getattr.md)
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [_bt_advance_array_keys](_bt_advance_array_keys.md)
  - ScanDirectionIsForward/ScanDirectionIsBackward
- Called from (representative examples):
  - [_bt_checkkeys](_bt_checkkeys.md)
  - [_bt_advance_array_keys](_bt_advance_array_keys.md)

## Notes and Other Information
- Handles complex scan key logic including required vs non-required keys in different directions
- Optimizes NULL handling based on NULLS FIRST/LAST ordering
- Critical for B-tree scan performance through early termination logic
- Special handling for truncated attributes in pivot tuples
- Part of the PostgreSQL B-tree access method implementation in src/backend/access/nbtree/nbtutils.c:3682-3887