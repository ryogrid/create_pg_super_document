# AlterObjectNamespace_oid

## Location
src/backend/commands/alter.c: 614 - 680

## Overview
Changes an object's namespace given its class OID and object OID, primarily designed for ALTER EXTENSION SET SCHEMA operations.

## Definition


## Detailed Description
AlterObjectNamespace_oid provides a low-level interface for moving database objects between schemas when you have the object's class OID and object OID. This function is specifically designed to support ALTER EXTENSION SET SCHEMA operations, which need to move multiple objects that belong to an extension to a new schema.

The function handles different object types through a switch statement based on the class ID:
1. **Relations** (RelationRelationId): Uses specialized table namespace alteration
2. **Types** (TypeRelationId): Delegates to type-specific namespace alteration  
3. **Generic objects**: Functions, collations, operators, etc. use the common internal alteration logic
4. **Unsupported objects**: Silently ignored if they don't have schema-qualified names

The function is designed to work with dependent types and objects, allowing the caller to track moved objects through the objsMoved parameter. Objects without schemas or dependent types that should be ignored return InvalidOid.

## Parameters / Member Variables
- : The OID of the object's system catalog (e.g., RelationRelationId, TypeRelationId)
- : The OID of the specific object to move
- : The OID of the target namespace/schema
- : Pointer to ObjectAddresses structure for tracking moved objects during extension operations

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