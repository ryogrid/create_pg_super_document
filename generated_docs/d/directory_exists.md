# directory_exists

## Location
[src/test/regress/pg_regress.c:1313-1325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1313-L1325)

## Overview
A utility function that checks whether a specified path exists and is a directory.

## Definition

```c
struct stat st;
```
## Detailed Description
The  function uses the POSIX  system call to retrieve file system information about the specified path and then checks if the path corresponds to a directory. The function first attempts to get the file status information using . If this call fails (indicating the path doesn't exist or cannot be accessed), the function returns false. If the stat call succeeds, it uses the  macro to test whether the file mode indicates a directory.

This function is part of the PostgreSQL regression testing infrastructure and provides a reliable way to verify directory existence before attempting directory operations.

## Parameters / Member Variables

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
base.comparetup_tiebreak
base.onlyKey
bgwriter
bigint_value2
bos[a-
bra
bv_allnulls
c
cachectx
client_finished_auth
co]
colname
config
configure
configure.ac
context
contiguous_pages
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
deadTupleStorage
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
high.y
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
low.y
lower
lz4_compression_init_doc.md
lz4_stream_eof_doc.md
member
meson.build
meson_options.txt
ndims
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
point
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
write_data_to_archive_lz4_doc.md: A null-terminated string containing the path to the directory whose existence is to be checked

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (POSIX system call for getting file status)
  - S_ISDIR (POSIX macro for testing if a file mode represents a directory)
- Called from (representative examples):
  - [TAPtype](../T/TAPtype.md) (used in TAP test type detection)
  - [open_result_files](../o/open_result_files.md) (used to verify result directories exist before opening files)
  - [regression_main](../r/regression_main.md) (used to check for various test directories during setup)

## Notes and Other Information
- Returns true if the path exists and is a directory
- Returns false if the path does not exist, cannot be accessed, or is not a directory
- Uses the POSIX  system call, making it portable across Unix-like systems
- The function is static, meaning it's only accessible within the pg_regress.c file
- Part of the PostgreSQL regression testing infrastructure
- Distinguishes between non-existent paths and paths that exist but are not directories
- Handles permission issues gracefully - if  fails due to permissions, returns false
- More robust than attempting to open a directory, as it specifically tests for directory type