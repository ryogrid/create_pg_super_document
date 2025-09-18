# HTAB

## Location
[src/backend/utils/hash/dynahash.c:219-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L219-L243)

## Overview
HTAB is the top-level control structure for PostgreSQL's hash tables, containing function pointers, memory management information, and local copies of frequently accessed values.

## Definition


## Detailed Description
HTAB serves as the primary interface structure for PostgreSQL's dynamic hash tables. In shared-memory hash tables, each backend maintains its own copy of the HTAB structure (which is safe since no fields change at runtime), while the actual shared control information resides in the HASHHDR structure pointed to by hctl.

The structure contains function pointers for hash operations, memory management context, and local copies of frequently accessed values to reduce contention on shared memory. It supports both shared and non-shared hash tables with configurable sizing and allocation strategies.

## Parameters / Member Variables
- : Pointer to HASHHDR structure containing shared control information
- 0					frametailpos
5					glob-
=					global_symbols.db
FuzzyAttrMatchState_documentation.md	import_symbol_reference.py
Pfdebug					inh
R					initial_rels
README.md				log
T2					lsn[]
W					maxParallelHazard
__pycache__				output
area					p_next_resno
attnums					parent
base.nKeys				parent_relid
baserestrictcost			process_symbol_definitions.py
blockState				processed_tlist
canon_pathkeys				ri_ChildToRootMap
contrib					ri_ReturningSlot
create_duckdb_index.py			ri_TrigNewSlot
curTransactionContext			ri_TrigOldSlot
curaggcontext				rows
data					rs_ctup.t_data
ec_merging_done				scripts
es_query_cxt				set_file_end_lines.py
estimate				src
extract_readme_file_header_comments.py	state
extract_symbol_references.py		syncrep_method
filter_frequent_symbol_from_csv.py	temp_slot_2
framehead_slot				type
frameheadpos				update_colnos
frametail_slot				update_symbol_types.py: Directory array pointing to the start of each hash table segment (HASHSEGMENT)
- hash: hash table empty: Function pointer for computing hash values from keys (HashValueFunc)
- : Function pointer for comparing hash keys (HashCompareFunc)
- : Function pointer for copying hash keys (HashCopyFunc)
- : Function pointer for memory allocation (HashAllocFunc)
- : Memory context used when the default allocator is employed
- : String name of the table used in error messages and debugging
- : Boolean flag indicating if the table resides in shared memory
- : Boolean flag indicating if table size is fixed (no automatic enlargement)
- : Boolean flag indicating if new inserts are prohibited (only valid for non-shared tables)
- : Local copy of hash key length in bytes (reduces contention)
- : Local copy of segment size, must be power of 2 (reduces contention)
- : Local copy of segment shift value, calculated as log2(ssize) (reduces contention)

## Dependencies
- Functions called/Symbols referenced:
  - [HASHHDR](HASHHDR.md) (pointed to by hctl)
  - HASHSEGMENT (pointed to by dir)
- Called from (representative examples):
  - [hash_create](../h/hash_create.md)
  - [hash_destroy](../h/hash_destroy.md)
  - [hash_search](../h/hash_search.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_freeze](../h/hash_freeze.md)
  - [expand_table](../e/expand_table.md)

## Notes and Other Information
- In shared-memory configurations, each backend has its own HTAB copy while sharing HASHHDR data
- The frozen flag can only be set for non-shared tables since shared table freezing requires coordination
- Local copies of keysize, ssize, and sshift are maintained to reduce contention on shared memory
- Function pointers allow for customizable hash, comparison, copying, and allocation behaviors
- The structure is defined at src/backend/utils/hash/dynahash.c:219-243
- [HTAB](HTAB.md) instances are extensively used throughout PostgreSQL for caching, lookup tables, and internal data structures