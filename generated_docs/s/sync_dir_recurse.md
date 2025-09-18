# sync_dir_recurse

## Location
src/common/file_utils.c: 220 - 270

## Overview
A convenient wrapper function that synchronizes a directory and all its contents using either syncfs() or fsync() methods depending on the specified synchronization method.

## Definition


## Detailed Description
The  function provides a simplified interface for synchronizing a directory and all its contents. It abstracts the complexity of choosing between different synchronization methods and handles the platform-specific implementation details.

The function supports the same two synchronization methods as :
1. **SYNCFS method**: Uses the Linux-specific syncfs() system call to synchronize the entire filesystem containing the directory
2. **FSYNC method**: Uses walkdir() to traverse the directory tree and fsync() individual files, with optional pre-sync hinting for performance

Unlike , this function doesn't handle symbolic links specially - it processes directories recursively but doesn't follow symlinks by default.

## Parameters / Member Variables
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
copy_already_done
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
looids[]
lower
lz4_compression_init_doc.md
lz4_stream_eof_doc.md
member
meson.build
meson_options.txt
nsubs
ntuples
number_of_rows
numlos
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
range2
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
write_data_to_archive_lz4_doc.md: Path to the directory to synchronize recursively
- : Synchronization method to use (DATA_DIR_SYNC_METHOD_SYNCFS or DATA_DIR_SYNC_METHOD_FSYNC)

## Dependencies
- Functions called/Symbols referenced:
  - [do_syncfs](../d/do_syncfs.md)
  - [walkdir](../w/walkdir.md)
  - [pre_sync_fname](../p/pre_sync_fname.md)
  - [fsync_fname](../f/fsync_fname.md)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup)
  - [_CloseArchive](../C/_CloseArchive.md) (pg_dump)

## Notes and Other Information
- This is a wrapper function designed for simpler use cases where special symlink handling is not required
- The syncfs method is Linux-specific and requires HAVE_SYNCFS compile-time support
- When using fsync method, performs pre-sync operations when PG_FLUSH_DATA_WORKS is available
- Does not follow symbolic links (walkdir called with process_symlinks = false)
- Used by utilities like pg_basebackup and pg_dump for ensuring data durability
- More straightforward than sync_pgdata but less feature-rich for complex PostgreSQL data directory layouts