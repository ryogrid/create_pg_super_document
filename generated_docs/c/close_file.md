# close_file

## Location
[src/timezone/zic.c:527-542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L527-L542)

## Overview
A safe file closing utility function in the timezone compiler that properly handles file closure with comprehensive error checking and reporting.

## Definition

```c
static void
close_file(FILE *stream, char const *dir, char const *name)
```
## Detailed Description
This function provides robust file closing functionality for the timezone compiler (zic). It performs comprehensive error checking before and during file closure, including checking for I/O errors that may have occurred during previous operations on the file stream. The function ensures that any errors encountered during file operations are properly reported with contextual information about the file location.

The function first checks if there were any I/O errors on the stream using , then attempts to close the file with . If either operation indicates an error, it constructs a detailed error message that includes the program name, directory path, filename, and the specific error description. Upon encountering any error, the function terminates the program with .

## Parameters / Member Variables
- : FILE pointer to the stream to be closed
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
write_data_to_archive_lz4_doc.md: Directory path (can be NULL) where the file is located
- : Filename (can be NULL) being closed

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function)
  -  (standard C library function)
  -  (standard C library function)
  -  (standard C library function)
  -  (standard C library function)
  -  (standard constant)
  -  (global program name variable)
  -  (global error variable)
- Called from (representative examples):
  -  (in zic.c:554)
  -  (in zic.c:676)
  -  (in zic.c:1090-1091)
  -  (in zic.c:1351)

## Notes and Other Information
- Located in 
- This function is declared as , limiting its scope to the zic.c file
- Part of the timezone data compilation infrastructure
- Uses internationalization with the  macro for error messages
- Provides a "fail-fast" approach by terminating the program on any file operation error
- Handles NULL values for 0
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
write_data_to_archive_lz4_doc.md and  parameters gracefully in error message construction
- The error message format provides full context: program name, file path, filename, and error description
- Demonstrates defensive programming by checking for both stream errors and close operation failures