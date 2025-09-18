# set_rtable_names

## Location
[src/backend/utils/adt/ruleutils.c:3828-3972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3828-L3972)

## Overview
Selects unique RTE (Range Table Entry) aliases to be used when printing a query, ensuring name uniqueness across current and parent namespaces.

## Definition
static void set_rtable_names(deparse_namespace *dpns, List *parent_namespaces, Bitmapset *rels_used)

## Detailed Description
This function fills in the rtable_names list within a deparse_namespace structure, creating a one-to-one mapping with the rtable list. Each RTE name is guaranteed to be unique among those in the new namespace plus any ancestor namespaces. The function uses a hash table for efficient O(N) performance instead of O(N^2). When name conflicts occur, it appends numeric suffixes (_1, _2, etc.) to create unique names, and handles name length constraints by truncating if necessary while preserving the numeric suffix.

## Parameters / Member Variables
- dpns: Pointer to deparse_namespace structure to be populated with table names
- parent_namespaces: List of ancestor namespace contexts to avoid name conflicts with
- rels_used: Bitmapset indicating which RTE indexes should be given aliases (can be NULL to process all)

## Dependencies
- Functions called/Symbols referenced:
  - HASHCTL
  - [HTAB](../H/HTAB.md)
  - NameHashEntry
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [hash_destroy](../h/hash_destroy.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [pg_mbcliplen](../p/pg_mbcliplen.md)
- Called from (representative examples):
  - [select_rtable_names_for_explain](select_rtable_names_for_explain.md)
  - [set_deparse_for_query](set_deparse_for_query.md)
  - [deparse_context_for](../d/deparse_context_for.md)
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)

## Notes and Other Information
- This function is static and only concerned with relation names, not column names
- Uses a hash table with NAMEDATALEN key size for efficient name lookup and conflict resolution
- Preloads the hash table with names from parent namespaces to avoid conflicts
- Handles various RTE kinds: user-defined aliases, relation names, joins, and parser-assigned names
- For unnamed joins, assigns NULL as the refname
- Includes interrupt checking for potentially long operations
- Memory management includes proper cleanup of the hash table at function end