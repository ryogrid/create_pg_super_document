# make_directory

## Location
src/test/regress/pg_regress.c: 1326 - 1335

## Overview
A utility function that creates a directory with full permissions for all users (owner, group, and others).

## Definition


## Detailed Description
The  function creates a new directory using the POSIX  system call with comprehensive permissions. The directory is created with read, write, and execute permissions for the owner, group, and others (mode 0777). If the directory creation fails for any reason (such as the directory already existing, insufficient permissions, or invalid path), the function calls  to terminate the program with an error message.

This function is part of the PostgreSQL regression testing infrastructure and is used to ensure that necessary directories exist before attempting to write test files or results.

## Parameters / Member Variables
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
write_data_to_archive_lz4_doc.md: A null-terminated string containing the path of the directory to be created

## Dependencies
- Functions called/Symbols referenced:
  - mkdir (POSIX system call for creating directories)
  - S_IRWXU (POSIX constant for owner read/write/execute permissions)
  - S_IRWXG (POSIX constant for group read/write/execute permissions)  
  - S_IRWXO (POSIX constant for others read/write/execute permissions)
  - bail (PostgreSQL regression test function for fatal error handling)
- Called from (representative examples):
  - TAPtype (used in TAP test type setup)
  - [open_result_files](../o/open_result_files.md) (used to create result directories before opening files)
  - [regression_main](../r/regression_main.md) (used to create various test directories during setup)

## Notes and Other Information
- Creates directories with mode 0777 (read/write/execute for owner, group, and others)
- Uses  for error handling, which terminates the program on failure
- The function is static, meaning it's only accessible within the pg_regress.c file
- Part of the PostgreSQL regression testing infrastructure
- Does not check if the directory already exists - relies on  to handle this case
- The function will fail if any parent directories in the path don't exist (does not create parent directories)
- Uses POSIX permission constants for maximum portability
- Fatal error handling ensures that test execution stops if directory creation fails