# ExecAlterOwnerStmt

## Location
[src/backend/commands/alter.c:826-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L826-L916)

## Overview
Executes an ALTER OBJECT OWNER TO statement by dispatching to the appropriate type-specific owner alteration function based on the object type.

## Definition

```c
ObjectAddress
ExecAlterOwnerStmt(AlterOwnerStmt *stmt)
```
## Detailed Description
ExecAlterOwnerStmt serves as the central dispatcher for ALTER OBJECT OWNER TO statements in PostgreSQL. It first resolves the new owner's role specification to an OID, then examines the object type in the statement to route the operation to the appropriate specialized function for handling ownership changes for that particular object type.

The function handles two main categories of objects:
1. **Special cases**: Databases, schemas, types/domains, foreign data wrappers, foreign servers, event triggers, publications, and subscriptions have dedicated owner alteration functions
2. **Generic path**: Most other objects (functions, operators, collations, etc.) use a common approach via 

For generic objects, the function resolves the object address using  with exclusive locking, then delegates to the internal owner alteration function. The function ensures that complex objects requiring special handling get routed to their appropriate specialized functions.

## Parameters / Member Variables
- : Pointer to the parsed ALTER OWNER statement containing the object type, object identifier, and new owner role specification

## Dependencies
- Functions called/Symbols referenced:
  - : Resolves role specification to OID for the new owner
  - : Database-specific owner alteration
  - : Schema-specific owner alteration  
  - : Type and domain owner alteration
  - : FDW owner alteration
  - : Foreign server owner alteration
  - : Event trigger owner alteration
  - : Publication owner alteration
  - : Subscription owner alteration
  - : Generic object address resolution for generic cases
  - : Core ownership alteration logic for generic objects
- Called from (representative examples):
  - : Main utility command processing
  - : Utility command processing path

## Notes and Other Information
- Returns the ObjectAddress of the object whose ownership was changed
- Uses AccessExclusiveLock when resolving object addresses for ownership changes
- Handles a wide variety of object types through a comprehensive switch statement
- Special object types get routed to dedicated functions that handle their specific requirements
- Generic object types use a common path through 
- Role specification resolution occurs once at the beginning to get the new owner OID
- Error handling includes elog(ERROR) for unrecognized object types
- The function assumes the caller has appropriate permissions to execute the ownership change

## Simplified Source

```c
ObjectAddress ExecAlterOwnerStmt(AlterOwnerStmt *stmt) {
    Oid newowner = get_rolespec_oid(stmt->newowner, false);

    switch (stmt->objectType) {
        // Database objects
        case OBJECT_DATABASE:
            return AlterDatabaseOwner(strVal(stmt->object), newowner);

        // Schema objects
        case OBJECT_SCHEMA:
            return AlterSchemaOwner(strVal(stmt->object), newowner);

        // Type and domain objects
        case OBJECT_TYPE:
        case OBJECT_DOMAIN:
            return AlterTypeOwner(castNode(List, stmt->object), newowner, stmt->objectType);

        // Foreign data wrapper objects
        case OBJECT_FDW:
            return AlterForeignDataWrapperOwner(strVal(stmt->object), newowner);

        case OBJECT_FOREIGN_SERVER:
            return AlterForeignServerOwner(strVal(stmt->object), newowner);

        // Event trigger objects
        case OBJECT_EVENT_TRIGGER:
            return AlterEventTriggerOwner(strVal(stmt->object), newowner);

        // Publication objects
        case OBJECT_PUBLICATION:
            return AlterPublicationOwner(strVal(stmt->object), newowner);

        // Subscription objects
        case OBJECT_SUBSCRIPTION:
            return AlterSubscriptionOwner(strVal(stmt->object), newowner);

        // Generic objects
        case OBJECT_AGGREGATE:
        case OBJECT_COLLATION:
        case OBJECT_CONVERSION:
        case OBJECT_FUNCTION:
        case OBJECT_LANGUAGE:
        case OBJECT_LARGEOBJECT:
        case OBJECT_OPERATOR:
        case OBJECT_OPCLASS:
        case OBJECT_OPFAMILY:
        case OBJECT_PROCEDURE:
        case OBJECT_ROUTINE:
        case OBJECT_STATISTIC_EXT:
        case OBJECT_TABLESPACE:
        case OBJECT_TSDICTIONARY:
        case OBJECT_TSCONFIGURATION:
        {
            Relation relation;
            ObjectAddress address;

            // Resolve object address
            address = get_object_address(stmt->objectType, stmt->object,
                                       &relation, AccessExclusiveLock, false);
            Assert(relation == NULL);

            // Change ownership
            AlterObjectOwner_internal(address.classId, address.objectId, newowner);

            return address;
        }

        default:
            elog(ERROR, "unrecognized AlterOwnerStmt type: %d", (int) stmt->objectType);
            return InvalidObjectAddress;
    }
}
```