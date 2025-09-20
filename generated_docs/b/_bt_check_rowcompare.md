# _bt_check_rowcompare

## Location
[src/backend/access/nbtree/nbtutils.c:3888-4071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L3888-L4071)

## Overview
Tests whether an index tuple satisfies a row-comparison scan condition by performing column-by-column comparisons for composite key conditions.

## Definition

```c
static bool
_bt_check_rowcompare(ScanKey skey, IndexTuple tuple, int tupnatts,
					 TupleDesc tupdesc, ScanDirection dir, bool *continuescan)
```
## Detailed Description
This function handles row-comparison operations for composite B-tree index keys, where multiple columns are compared as a single logical unit (e.g., WHERE (col1, col2) > (val1, val2)). It performs a lexicographic comparison by iterating through each column in the row until it finds a decisive difference or reaches the end. The function handles NULL values according to the index's NULLS FIRST/LAST configuration and can optimize scans by setting *continuescan to false when no future tuples can satisfy required conditions. For each column, it performs a three-way comparison and applies the appropriate strategy (less than, greater than, etc.) to determine the final result.

## Parameters / Member Variables
- : Scan key containing the row-comparison condition (SK_ROW_HEADER)
- : Index tuple being tested against the row condition
- : Number of attributes in the tuple
- : Tuple descriptor defining attribute types and properties
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
- : Output flag indicating whether scan should continue

## Dependencies
- Functions called/Symbols referenced:
  - [index_getattr](../i/index_getattr.md)
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - ScanDirectionIsForward/ScanDirectionIsBackward
  - INVERT_COMPARE_RESULT
- Called from (representative examples):
  - [_bt_check_compare](_bt_check_compare.md)

## Notes and Other Information
- Handles truncated attributes in pivot tuples by assuming they pass the qualification
- Uses three-way comparison functions rather than boolean operators for efficiency
- Supports DESC columns through result inversion via INVERT_COMPARE_RESULT
- Critical for multi-column index performance in PostgreSQL
- Part of the B-tree access method's scan key evaluation system
- Located in src/backend/access/nbtree/nbtutils.c:3888-4071