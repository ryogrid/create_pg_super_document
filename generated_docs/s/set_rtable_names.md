# set_rtable_names

## Location
src/backend/utils/adt/ruleutils.c: 3828 - 3972

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
  - HTAB
  - NameHashEntry
  - hash_create
  - hash_search
  - hash_destroy
  - bms_is_member
  - get_rel_name
  - pg_mbcliplen
- Called from (representative examples):
  - select_rtable_names_for_explain
  - set_deparse_for_query
  - deparse_context_for
  - pg_get_triggerdef_worker

## Notes and Other Information
- This function is static and only concerned with relation names, not column names
- Uses a hash table with NAMEDATALEN key size for efficient name lookup and conflict resolution
- Preloads the hash table with names from parent namespaces to avoid conflicts
- Handles various RTE kinds: user-defined aliases, relation names, joins, and parser-assigned names
- For unnamed joins, assigns NULL as the refname
- Includes interrupt checking for potentially long operations
- Memory management includes proper cleanup of the hash table at function end