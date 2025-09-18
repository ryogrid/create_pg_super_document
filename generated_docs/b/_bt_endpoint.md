# _bt_endpoint

## Location
src/backend/access/nbtree/nbtsearch.c: 2573 - 2662

## Overview
Finds the first or last page in a B-tree index and scans from there to locate the first key satisfying all search qualifications for index scan initialization.

## Definition


## Detailed Description
This function implements the endpoint scanning strategy for B-tree index scans when the scan must start at the very beginning or end of the index. It serves as a specialized version of the general search algorithm, optimized for cases where no key-based positioning is needed. The function handles both forward scans (starting from leftmost page) and backward scans (starting from rightmost page).

The algorithm performs a simplified tree descent without maintaining a search stack, since endpoint operations don't require backtracking. It establishes appropriate predicate locks for MVCC compliance and handles the initial page reading and positioning. The function also manages the transition from finding the endpoint page to actually reading valid data, including advancing to subsequent pages when the endpoint page contains no qualifying tuples.

## Parameters / Member Variables
- : IndexScanDesc - The index scan descriptor containing scan configuration and state
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
typescript
update_symbol_types.py
usesspi
v
variable
venv
views
wrds
writeData
write_data_to_archive_lz4_doc.md: ScanDirection - The scan direction (forward from left endpoint, backward from right endpoint)

## Dependencies
- Functions called/Symbols referenced:
  - _bt_get_endpoint
  - ScanDirectionIsBackward
  - ScanDirectionIsForward
  - PredicateLockRelation
  - PredicateLockPage
  - BTScanPosInvalidate
  - _bt_initialize_more_data
  - _bt_readpage
  - _bt_unlockbuf
  - _bt_steppage
  - _bt_drop_lock_and_maybe_pin
  - BTPageGetOpaque
  - P_ISLEAF
  - P_FIRSTDATAKEY
  - P_RIGHTMOST
  - PageGetMaxOffsetNumber
  - BufferGetBlockNumber
  - BTScanOpaque (type)
  - BTPageOpaque (type)
  - BTScanPosItem (type)
- Called from (representative examples):
  - _bt_first

## Notes and Other Information
- This is a static function only accessible within nbtsearch.c
- Returns true if valid data is found, false if the index is empty or no qualifying tuples exist
- Handles empty index case by locking the entire relation since no finer-grained lock target exists
- Establishes predicate locks on the endpoint page for proper MVCC behavior
- Uses simplified search logic since no key-based positioning or stack maintenance is required
- For forward scans, starts from P_FIRSTDATAKEY; for backward scans, starts from the maximum offset
- Includes error handling for invalid scan directions
- May advance to subsequent pages if the endpoint page contains no qualifying data
- Sets up the scan state including heap tuple ID and index tuple references for the first qualifying item