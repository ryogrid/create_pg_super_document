# AlterObjectNamespace_oid

## Location
[src/backend/commands/alter.c:614-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L614-L680)

## Overview
Changes an object's namespace given its class OID and object OID, primarily designed for ALTER EXTENSION SET SCHEMA operations.

## Definition

```c
Oid
AlterObjectNamespace_oid(Oid classId, Oid objid, Oid nspOid,
						 ObjectAddresses *objsMoved)
```
## Detailed Description
AlterObjectNamespace_oid provides a low-level interface for moving database objects between schemas when you have the object's class OID and object OID. This function is specifically designed to support ALTER EXTENSION SET SCHEMA operations, which need to move multiple objects that belong to an extension to a new schema.

The function handles different object types through a switch statement based on the class ID:
1. **Relations** (RelationRelationId): Uses specialized table namespace alteration
2. **Types** (TypeRelationId): Delegates to type-specific namespace alteration  
3. **Generic objects**: Functions, collations, operators, etc. use the common internal alteration logic
4. **Unsupported objects**: Silently ignored if they don't have schema-qualified names

The function is designed to work with dependent types and objects, allowing the caller to track moved objects through the objsMoved parameter. Objects without schemas or dependent types that should be ignored return InvalidOid.

## Parameters / Member Variables
- `classId`: The OID of the object's system catalog (e.g., RelationRelationId, TypeRelationId)
- `objid`: The OID of the specific object to move
- `nspOid`: The OID of the target namespace/schema
- `*objsMoved`: Pointer to ObjectAddresses structure for tracking moved objects during extension operations
## Dependencies
- Functions called/Symbols referenced:
  - : Opens relations with AccessExclusiveLock
  - : Closes relations 
  - : Gets the current namespace of a relation
  - : Internal table namespace alteration
  - : Type-specific namespace alteration
  - : Generic namespace alteration logic
  - : Validates if object type supports schemas
- Called from (representative examples):
  - : Extension schema alteration operations

## Notes and Other Information
- Returns the OID of the object's previous namespace, or InvalidOid if the object doesn't have a schema
- Currently used primarily by ALTER EXTENSION SET SCHEMA functionality
- Silently ignores dependent types, assuming they will be moved with their parent objects  
- Uses assertions to verify that ignored object types truly don't have schema-qualified names
- Designed to handle bulk operations efficiently as part of extension schema changes
- Uses appropriate locking (AccessExclusiveLock for relations, RowExclusiveLock for catalog access)

## Simplified Source

```c
Oid AlterObjectNamespace_oid(Oid classId, Oid objid, Oid nspOid, ObjectAddresses *objsMoved) {
    Oid oldNspOid = InvalidOid;

    switch (classId) {
        case RelationRelationId:
            // Handle table/relation namespace change
            rel = relation_open(objid, AccessExclusiveLock);
            oldNspOid = RelationGetNamespace(rel);
            AlterTableNamespaceInternal(rel, oldNspOid, nspOid, objsMoved);
            relation_close(rel, NoLock);
            break;

        case TypeRelationId:
            // Handle type namespace change
            oldNspOid = AlterTypeNamespace_oid(objid, nspOid, true, objsMoved);
            break;

        case ProcedureRelationId:
        case CollationRelationId:
        case ConversionRelationId:
        case OperatorRelationId:
        case OperatorClassRelationId:
        case OperatorFamilyRelationId:
        case StatisticExtRelationId:
        case TSParserRelationId:
        case TSDictionaryRelationId:
        case TSTemplateRelationId:
        case TSConfigRelationId:
            // Handle generic object namespace change
            catalog = table_open(classId, RowExclusiveLock);
            oldNspOid = AlterObjectNamespace_internal(catalog, objid, nspOid);
            table_close(catalog, RowExclusiveLock);
            break;

        default:
            // Ignore objects without schema-qualified names
            break;
    }

    return oldNspOid;
}
```