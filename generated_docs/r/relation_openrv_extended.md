# relation_openrv_extended

## Location
[src/backend/access/common/relation.c:172-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/relation.c#L172-L204)

## Overview
Extended version of relation_openrv that allows graceful handling of missing relations by returning NULL instead of raising an error when a relation is not found.

## Definition

```c
Relation
relation_openrv_extended(const RangeVar *relation, LOCKMODE lockmode,
						 bool missing_ok)
```
## Detailed Description
The `relation_openrv_extended` function extends the functionality of `relation_openrv` by adding a `missing_ok` parameter that controls error handling behavior when a relation cannot be found. This provides callers with the flexibility to handle missing relations gracefully without exception handling. The function follows the same core process as `relation_openrv`:

1. **Cache Invalidation Handling**: Processes shared-cache invalidation messages when lockmode is not NoLock to ensure current ACL information
2. **Namespace Resolution**: Uses RangeVarGetRelid with the missing_ok parameter to resolve the relation name, which may return InvalidOid if the relation is not found and missing_ok is true
3. **Missing Relation Handling**: Returns NULL immediately if the resolved OID is invalid, allowing callers to detect and handle missing relations
4. **Delegation**: For existing relations, delegates to relation_open for the complete opening process

This function is particularly useful for operations that need to conditionally work with relations that may or may not exist, such as DROP IF EXISTS statements.

## Parameters / Member Variables
- `relation`: Pointer to a RangeVar structure containing the relation name and optional schema qualification
- `lockmode`: The type of lock to acquire on the relation
- `missing_ok`: If true, return NULL when relation is not found; if false, raise an error

## Dependencies
- Functions called/Symbols referenced:
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) - Processes cache invalidation messages
  - RangeVarGetRelid - Resolves RangeVar to relation OID with locking and missing_ok behavior
  - [relation_open](relation_open.md) - Opens the relation by OID
  - [RangeVar](../R/RangeVar.md) - Structure type for relation name specification
  - OidIsValid - Checks if an OID is valid

- Called from (representative examples):
  - [table_openrv_extended](../t/table_openrv_extended.md) - Extended table opening with missing_ok
  - [get_relation_by_qualified_name](../g/get_relation_by_qualified_name.md) - Object address resolution with optional existence
  - [get_object_address_publication_rel](../g/get_object_address_publication_rel.md) - [Publication](../P/Publication.md) relation address resolution

## Notes and Other Information
- Returns NULL when missing_ok is true and the relation is not found, unlike relation_openrv which always raises an error
- Other types of errors (such as permission problems) will still result in exceptions even when missing_ok is true
- The missing_ok parameter is passed through to RangeVarGetRelid, which handles the actual existence checking
- Maintains the same cache invalidation logic as relation_openrv to ensure ACL consistency
- Useful for implementing IF EXISTS functionality in SQL commands
- The function provides a clean way to probe for relation existence while respecting lock ordering and cache consistency requirements

## Simplified Source

```c
Relation
relation_openrv_extended(const RangeVar *relation, LOCKMODE lockmode,
                        bool missing_ok)
{
    Oid relOid;

    // Handle cache invalidation if locking
    if (lockmode != NoLock)
        AcceptInvalidationMessages();

    // Look up relation by name with optional existence check
    relOid = RangeVarGetRelid(relation, lockmode, missing_ok);

    // Return NULL if not found and missing_ok is true
    if (!OidIsValid(relOid))
        return NULL;

    // Open relation using OID
    return relation_open(relOid, NoLock);
}
```