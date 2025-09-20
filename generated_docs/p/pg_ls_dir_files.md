# pg_ls_dir_files

## Location
[src/backend/utils/adt/genfile.c:570-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L570-L632)

## Overview
A generic internal function that lists regular files in a directory with detailed file information including size and modification time, used as the base implementation for various PostgreSQL directory listing functions.

## Definition

```c
struct dirent *de;
```
## Detailed Description
The  function is a static helper function that provides the core implementation for listing files in PostgreSQL system directories. Unlike the basic  function which only returns filenames, this function returns detailed information about each file including the filename, size, and modification time.

The function specifically filters for regular files only (not directories or special files) and skips hidden files (those starting with a dot). It performs comprehensive error handling for missing directories and files that may be deleted concurrently during iteration. The function is designed to complete directory reading within a single SRF call rather than keeping the directory open across multiple calls.

## Parameters / Member Variables
- : Function call information structure containing result set details
- 0
5
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
Makefile
README.md
README_PG.md
S[0]
__pycache__
aclocal.m4
assistive_info.db
bgwriter
bra
bv_allnulls
c
config
configure
configure.ac
contrib
create_duckdb_index.py
data
default
doc
err
extract_readme_file_header_comments.py
extract_symbol_references.py
filter_frequent_symbol_from_csv.py
filter_frequent_symbol_from_csv.py~
global_symbols.db
global_symbols_bf_add_symbol_type.db
import_symbol_reference.py
ket
l
lb
level
meson.build
meson_options.txt
nsubs
ntuples
output
p
p[z-
pgstat_subscription_flush_cb_doc.md
prevTail
process_symbol_definitions.py
process_symbol_definitions_illegular_records.txt
python_version
r
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
update_symbol_types.py
v
venv
views: Directory path to list (pre-validated by caller)
- : If true, returns empty result instead of error when directory doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initializes set-returning function with custom tuple descriptor)
  - AllocateDir (opens directory for reading)
  - ReadDir (reads directory entries)
  - [stat](../s/stat.md) (gets file attributes for size and modification time)
  - S_ISREG (macro to check if file is a regular file)
  - [Int64GetDatum](../I/Int64GetDatum.md), TimestampTzGetDatum (convert file attributes to PostgreSQL datum types)
  - [time_t_to_timestamptz](../t/time_t_to_timestamptz.md) (converts Unix timestamp to PostgreSQL timestamp)
  - tuplestore_putvalues (adds result rows with 3 columns to output)
  - FreeDir (closes directory handle)
  - ReturnSetInfo, DIR, dirent (data structures)
- Called from (representative examples):
  - [pg_ls_logdir](pg_ls_logdir.md) (lists log directory files)
  - [pg_ls_waldir](pg_ls_waldir.md) (lists WAL directory files)
  - [pg_ls_tmpdir](pg_ls_tmpdir.md) (lists temporary directory files)
  - [pg_ls_replslotdir](pg_ls_replslotdir.md) (lists replication slot directory files)

## Notes and Other Information
- This is a static function used internally by other pg_ls_* functions, not directly accessible from SQL
- Returns 3-column result set: filename (text), size (bigint), modification time (timestamptz)
- Automatically filters out directories, special files, and hidden files starting with '.'
- Handles concurrent file deletion gracefully by continuing when ENOENT is encountered during stat()
- Uses MAXPGPATH * 2 buffer size for constructed file paths to handle long directory and file names
- Completes all directory reading within a single function call for reliability
- Error reporting uses PostgreSQL's standard ereport mechanism with file access error codes