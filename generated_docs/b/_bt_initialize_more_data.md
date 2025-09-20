# _bt_initialize_more_data

## Location
[src/backend/access/nbtree/nbtsearch.c:2663-2685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L2663-L2685)

## Overview
Initializes the moreLeft, moreRight directional flags and resets scan state counters based on the current scan direction and primary scan requirements.

## Definition

```c
static inline void
_bt_initialize_more_data(BTScanOpaque so, ScanDirection dir)
```
## Detailed Description
This utility function sets up the directional scanning state for B-tree index scans by configuring the moreLeft and moreRight flags that control scan advancement logic. The function handles different scanning scenarios: primary scans with array keys (which can advance in both directions), forward scans (which can only advance right), and backward scans (which can only advance left).

For primary scans involving array keys, both directions are initially enabled to allow the scan to explore the full range of potential matches. For regular directional scans, only the appropriate direction is enabled. The function also performs defensive resets of scan state counters to ensure clean initialization.

## Parameters / Member Variables
- : BTScanOpaque - The B-tree scan opaque structure containing scan state
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
write_data_to_archive_lz4_doc.md: ScanDirection - The direction of the scan (forward or backward)

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - BTScanOpaque (type)
  - ScanDirection (type)
- Called from (representative examples):
  - [_bt_first](_bt_first.md)
  - [_bt_parallel_readpage](_bt_parallel_readpage.md)
  - [_bt_endpoint](_bt_endpoint.md)

## Notes and Other Information
- This is a static inline function for performance, only accessible within nbtsearch.c
- The needPrimScan flag is reset to false after primary scan initialization
- Handles special case of array key scans which require bidirectional capability initially
- Sets numKilled and markItemIndex to defensive values (0 and -1 respectively) as paranoia measures
- The moreLeft/moreRight flags are used by other scan functions to determine when to stop advancing in each direction
- Part of the scan state management infrastructure for B-tree index operations
- Essential for proper scan termination and direction control in complex scanning scenarios