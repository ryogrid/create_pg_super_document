# get_object_address

## Location
[src/backend/catalog/objectaddress.c:922-1219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L922-L1219)

## Overview
Translates an object name and arguments (as passed by the parser) to an ObjectAddress, handling various PostgreSQL database objects with proper locking mechanisms.

## Definition

```c
ObjectAddress
get_object_address(ObjectType objtype, Node *object,
				   Relation *relp, LOCKMODE lockmode, bool missing_ok)
```
## Detailed Description
The  function is the central dispatcher for object name resolution in PostgreSQL. It takes a parsed object specification and converts it into a standardized ObjectAddress structure that uniquely identifies any database object. The function handles a comprehensive range of PostgreSQL objects including relations, functions, operators, types, constraints, and many others.

The function implements a retry mechanism to handle concurrent DDL operations that might invalidate object lookups between name resolution and locking. It uses shared invalidation message counters to detect when retries are necessary. For relation-based objects, the function also opens the relation and returns it through the  parameter, applying appropriate locking based on the object type.

The function performs different locking strategies depending on the object type: relations and attributes use the specified lock mode, while other child objects of relations acquire only AccessShareLock on the parent relation. Non-relation objects are locked using either shared or database-specific locking mechanisms.

## Parameters / Member Variables
- : The type of object being looked up (from ObjectType enumeration)
- : Parse tree node containing the object specification (name, arguments, etc.)
- : Output parameter that receives an open relation if the object is relation-related, NULL otherwise
- : The lock mode to apply to the target object (must not be NoLock)
- : If true, return invalid ObjectAddress instead of throwing error when object not found

## Dependencies
- Functions called/Symbols referenced:
  - [get_relation_by_qualified_name](get_relation_by_qualified_name.md) (for relation objects)
  - [get_object_address_attribute](get_object_address_attribute.md) (for attributes/columns)
  - [get_object_address_relobject](get_object_address_relobject.md) (for relation-dependent objects like triggers, rules)
  - [get_object_address_unqualified](get_object_address_unqualified.md) (for simple named objects)
  - [get_object_address_type](get_object_address_type.md) (for types and domains)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md) (for functions/procedures)
  - [LookupOperWithArgs](../L/LookupOperWithArgs.md) (for operators)
  - Various get_*_oid functions for specific object types
  - [LockSharedObject](../L/LockSharedObject.md)/LockDatabaseObject (for locking)
  - SharedInvalidMessageCounter (for concurrency control)
- Called from (representative examples):
  - [get_object_address_rv](get_object_address_rv.md)
  - [pg_get_object_address](../p/pg_get_object_address.md)
  - [ExecRenameStmt](../E/ExecRenameStmt.md)
  - [CommentObject](../C/CommentObject.md)
  - [RemoveObjects](../R/RemoveObjects.md)

## Notes and Other Information
The function uses a sophisticated retry loop to handle race conditions where concurrent DDL operations might change object mappings between lookup and locking. This is particularly important for maintaining consistency in a multi-user environment. The function is designed to be lock-safe and ensures that the returned ObjectAddress corresponds to a properly locked database object. The locking strategy varies by object type, with relation-based objects receiving more granular locking control than standalone objects.

## Simplified Source

```c
ObjectAddress
get_object_address(ObjectType objtype, Node *object,
                  Relation *relp, LOCKMODE lockmode, bool missing_ok)
{
    ObjectAddress address = {InvalidOid, InvalidOid, 0};
    ObjectAddress old_address = {InvalidOid, InvalidOid, 0};
    Relation relation = NULL;
    uint64 inval_count;

    Assert(lockmode != NoLock);

    // Retry loop to handle concurrent DDL changes
    for (;;) {
        // Remember invalidation count to detect concurrent changes
        inval_count = SharedInvalidMessageCounter;

        // Dispatch based on object type
        switch (objtype) {
            case OBJECT_TABLE:
            case OBJECT_VIEW:
            case OBJECT_SEQUENCE:
                address = get_relation_by_qualified_name(objtype, castNode(List, object),
                                                        &relation, lockmode, missing_ok);
                break;

            case OBJECT_ATTRIBUTE:
            case OBJECT_COLUMN:
                address = get_object_address_attribute(objtype, castNode(List, object),
                                                     &relation, lockmode, missing_ok);
                break;

            case OBJECT_FUNCTION:
            case OBJECT_PROCEDURE:
                address.classId = ProcedureRelationId;
                address.objectId = LookupFuncWithArgs(objtype, castNode(ObjectWithArgs, object), missing_ok);
                address.objectSubId = 0;
                break;

            case OBJECT_OPERATOR:
                address.classId = OperatorRelationId;
                address.objectId = LookupOperWithArgs(castNode(ObjectWithArgs, object), missing_ok);
                address.objectSubId = 0;
                break;

            case OBJECT_TYPE:
            case OBJECT_DOMAIN:
                address = get_object_address_type(objtype, castNode(TypeName, object), missing_ok);
                break;

            case OBJECT_DATABASE:
            case OBJECT_SCHEMA:
            case OBJECT_ROLE:
                address = get_object_address_unqualified(objtype,
                                                        castNode(String, object), missing_ok);
                break;

            // ... (handle other object types)
        }

        if (!address.classId)
            elog(ERROR, "unrecognized object type: %d", (int) objtype);

        // Return early if object not found and missing_ok is true
        if (!OidIsValid(address.objectId)) {
            Assert(missing_ok);
            return address;
        }

        // Check if we got the same result as before (retry logic)
        if (OidIsValid(old_address.classId)) {
            if (old_address.classId == address.classId &&
                old_address.objectId == address.objectId &&
                old_address.objectSubId == address.objectSubId)
                break;
            // Unlock old object if different
            // ... (unlock logic)
        }

        // Lock the object if it's not a relation (relations already locked)
        if (address.classId != RelationRelationId) {
            if (IsSharedRelation(address.classId))
                LockSharedObject(address.classId, address.objectId, 0, lockmode);
            else
                LockDatabaseObject(address.classId, address.objectId, 0, lockmode);
        }

        // Break if no invalidations occurred during our work
        if (inval_count == SharedInvalidMessageCounter || relation != NULL)
            break;

        old_address = address;
    }

    *relp = relation;
    return address;
}
```