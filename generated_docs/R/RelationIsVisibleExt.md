# RelationIsVisibleExt

## Location
src/backend/catalog/namespace.c: 925 - 994

## Overview
Extended version of RelationIsVisible that determines whether a relation is visible in the current search path, with additional error handling for missing relations.

## Definition
static bool RelationIsVisibleExt(Oid relid, bool *is_missing)

## Detailed Description
RelationIsVisibleExt performs the actual visibility checking logic for PostgreSQL relations. It first looks up the relation in the system cache and determines its namespace. The function then checks if the relation is visible in the current search path by:
1. First doing a quick check to see if the relation's namespace is in the active search path
2. If it's in the path, performing a detailed check to ensure no other relation with the same name appears earlier in the search path
3. Relations in the system catalog (PG_CATALOG_NAMESPACE) are always considered to be in the path

The key difference from RelationIsVisible is the optional error handling - if the relation is not found and is_missing is provided, it sets *is_missing = true instead of throwing an error.

## Parameters / Member Variables
- : The OID of the relation to check for visibility
- : Optional pointer to boolean flag; if provided and relation is not found, this will be set to true instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_class
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [get_relname_relid](../g/get_relname_relid.md)
- Called from (representative examples):
  - [RelationIsVisible](RelationIsVisible.md) (src/backend/catalog/namespace.c:915)
  - [pg_table_is_visible](../p/pg_table_is_visible.md) (src/backend/catalog/namespace.c:4900)

## Notes and Other Information
This is a static function that implements the core visibility logic. It carefully handles namespace precedence by iterating through the activeSearchPath and checking for name conflicts. The function properly manages system cache lookups and releases. Relations in the system namespace are treated specially as they are always considered visible.