# BTScanPosData

## Location
[src/include/access/nbtree.h:951-995](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L951-L995)

## Overview
BTScanPosData represents the complete state information needed for a B-tree index scan position, including buffer management, scan direction, item arrays, and page tracking data.

## Definition

```c
typedef struct BTScanPosData
{
	Buffer		buf;			/* if valid, the buffer is pinned */

	XLogRecPtr	lsn;			/* pos in the WAL stream when page was read */
	BlockNumber currPage;		/* page referenced by items array */
	BlockNumber nextPage;		/* page's right link when we scanned it */

	/*
	 * moreLeft and moreRight track whether we think there may be matching
	 * index entries to the left and right of the current page, respectively.
	 * We can clear the appropriate one of these flags when _bt_checkkeys()
	 * sets BTReadPageState.continuescan = false.
	 */
	bool		moreLeft;
	bool		moreRight;

	/*
	 * Direction of the scan at the time that _bt_readpage was called.
	 *
	 * Used by btrestrpos to "restore" the scan's array keys by resetting each
	 * array to its first element's value (first in this scan direction). This
	 * avoids the need to directly track the array keys in btmarkpos.
	 */
	ScanDirection dir;

	/*
	 * If we are doing an index-only scan, nextTupleOffset is the first free
	 * location in the associated tuple storage workspace.
	 */
	int			nextTupleOffset;

	/*
	 * The items array is always ordered in index order (ie, increasing
	 * indexoffset).  When scanning backwards it is convenient to fill the
	 * array back-to-front, so we start at the last slot and fill downwards.
	 * Hence we need both a first-valid-entry and a last-valid-entry counter.
	 * itemIndex is a cursor showing which entry was last returned to caller.
	 */
	int			firstItem;		/* first valid index in items[] */
	int			lastItem;		/* last valid index in items[] */
	int			itemIndex;		/* current index in items[] */

	BTScanPosItem items[MaxTIDsPerBTreePage];	/* MUST BE LAST */
} BTScanPosData;
```
## Detailed Description
This structure encapsulates all the state information required to track a scan position within a B-tree index. It manages buffer pins, tracks page relationships, maintains scan direction context, and holds arrays of matching items found on the current page. The structure supports both forward and backward scanning, with the items array filled accordingly. For index-only scans, it also manages tuple workspace offsets.

## Parameters / Member Variables
- : Buffer that is pinned if valid, providing access to the current page
- : XLogRecPtr position in the WAL stream when the page was read
- : BlockNumber of the page referenced by the items array
- : BlockNumber of the page's right link when it was scanned
- : Boolean flag indicating if there may be matching entries to the left
- : Boolean flag indicating if there may be matching entries to the right
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
write_data_to_archive_lz4_doc.md: ScanDirection of the scan when _bt_readpage was called
- : Integer tracking the first free location in tuple storage workspace for index-only scans
- : Integer index of the first valid entry in items array
- : Integer index of the last valid entry in items array
- : Integer cursor showing which entry was last returned to caller
- : Array of BTScanPosItem structures containing matching items (must be last member)

## Dependencies
- Functions called/Symbols referenced:
  - Buffer
  - XLogRecPtr
  - BlockNumber
  - ScanDirection
  - [BTScanPosItem](BTScanPosItem.md)
  - MaxTIDsPerBTreePage
- Called from (representative examples):
  - [btrestrpos](../b/btrestrpos.md)
  - [_bt_steppage](../b/_bt_steppage.md)
  - BTScanPos
  - [BTScanOpaqueData](BTScanOpaqueData.md)

## Notes and Other Information
- The items array MUST BE LAST due to variable-length considerations
- Items array is always ordered in index order (increasing indexoffset)
- For backward scans, the array is filled back-to-front for convenience
- Used for both current scan position and marked position in BTScanOpaqueData
- Critical for maintaining VACUUM synchronization through buffer pinning
- The moreLeft/moreRight flags optimize scan termination decisions