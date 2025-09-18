# nonword

## Location
[src/backend/regex/regcomp.c:1458-1475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1458-L1475)

## Overview
The nonword function generates NFA arcs for matching non-word-character positions ahead or behind the current position in regular expression processing.

## Definition


## Detailed Description
The nonword function is part of PostgreSQL's regular expression engine implementation. It creates arcs in the NFA (Non-deterministic Finite Automaton) that match non-word character positions. The function handles both lookahead (AHEAD) and lookbehind (BEHIND) assertions for non-word boundaries.

The function works by:
1. Setting up appropriate anchor characters ($ for AHEAD, ^ for BEHIND)
2. Creating new arcs with these anchor values
3. Using colorcomplement to handle the actual non-word character matching

## Parameters / Member Variables
- : Pointer to vars structure containing regex compilation context and state
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
end_compressor_lz4_doc.md
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
write_data_to_archive_lz4_doc.md: Direction flag - either AHEAD or BEHIND to specify lookahead or lookbehind
- : Left/source state pointer for the NFA arc
- : Right/destination state pointer for the NFA arc

## Dependencies
- Functions called/Symbols referenced:
  - AHEAD (constant)
  - BEHIND (constant)
  - [newarc](newarc.md)
  - [colorcomplement](../c/colorcomplement.md)
  - wordchrs (from vars structure)
- Called from (representative examples):
  - ARCV (multiple call sites in regcomp.c)

## Notes and Other Information
- This is a static function internal to the regex compilation module
- The function uses anchor characters ('$' and '^') to represent word boundaries
- No special handling is needed for newline characters in this context
- The function is used as part of word boundary assertion processing in regular expressions