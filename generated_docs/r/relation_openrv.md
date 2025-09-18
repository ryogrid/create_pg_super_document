# relation_openrv

## Location
src/backend/access/common/relation.c: 137 - 171

## Overview
Opens a relation specified by a RangeVar (relation name with optional schema qualification) rather than by OID, providing name-based relation access.

## Definition


## Detailed Description
The `relation_openrv` function provides a name-based interface to relation opening, accepting a RangeVar structure that contains the relation name and optional schema qualification. It performs namespace resolution to convert the name to an OID and then delegates to `relation_open` for the actual opening process. The function includes several important mechanisms:

1. **Cache Invalidation Handling**: Processes shared-cache invalidation messages before relation lookup to ensure current ACL information is visible, particularly important for GRANT/REVOKE operations that don't take relation locks
2. **Namespace Resolution**: Uses the namespace search path to resolve the relation name to its OID, respecting schema qualification if provided
3. **Lock Management**: Acquires the specified lock during namespace resolution, then passes NoLock to relation_open since the lock is already held
4. **Delegation**: Leverages relation_open for all the core functionality once the OID is determined

This function is the standard interface when relation names are available rather than OIDs, which is common in SQL command processing.

## Parameters / Member Variables
- `relation`: Pointer to a RangeVar structure containing the relation name and optional schema qualification
- `lockmode`: The type of lock to acquire on the relation

## Dependencies
- Functions called/Symbols referenced:
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) - Processes cache invalidation messages
  - RangeVarGetRelid - Resolves RangeVar to relation OID with locking
  - [relation_open](relation_open.md) - Opens the relation by OID
  - [RangeVar](../R/RangeVar.md) - Structure type for relation name specification

- Called from (representative examples):
  - table_openrv - Table-specific name-based opening
  - [get_object_address_attribute](../g/get_object_address_attribute.md) - Object address resolution
  - [get_object_address_attrdef](../g/get_object_address_attrdef.md) - Attribute default address resolution
  - [CreateStatistics](../C/CreateStatistics.md) - Statistics creation
  - [transformTableLikeClause](../t/transformTableLikeClause.md) - Table inheritance processing
  - [RelationNameGetTupleDesc](../R/RelationNameGetTupleDesc.md) - Tuple descriptor retrieval by name

## Notes and Other Information
- The function handles cache invalidation to ensure ACL changes are visible, which is crucial since GRANT/REVOKE don't take relation locks
- Skips invalidation message processing when NoLock is requested, assuming the caller has already ensured currency
- The RangeVar can specify just a relation name (using search_path) or be schema-qualified
- Lock acquisition happens during namespace resolution rather than in the final relation_open call
- This is the preferred interface for SQL commands that work with relation names rather than OIDs
- The design allows for proper namespace resolution while reusing all the core relation opening logic