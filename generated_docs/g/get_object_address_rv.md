# get_object_address_rv

## Location
[src/backend/catalog/objectaddress.c:1220-1241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1220-L1241)

## Overview
Returns an ObjectAddress based on a RangeVar and an object name, prepending the relation name to create a qualified object reference.

## Definition

```c
ObjectAddress
get_object_address_rv(ObjectType objtype, RangeVar *rel, List *object,
					  Relation *relp, LOCKMODE lockmode,
					  bool missing_ok)
```
## Detailed Description
The  function is a convenience wrapper around  that handles RangeVar-based object specifications. It constructs a fully qualified object name by prepending the relation information from the RangeVar to an existing object name list. This is particularly useful for finding objects that depend on a relation, such as constraints, triggers, or rules, where the object name needs to be qualified with the relation name.

The function builds a qualified name list by prepending the relation name, schema name (if specified), and catalog name (if specified) from the RangeVar to the provided object list. This creates a complete hierarchical name that can be processed by the main  function.

## Parameters / Member Variables
- : The type of object being looked up (from ObjectType enumeration)
- : RangeVar specifying the relation context (can be NULL)
- : List of object name components (may be empty)
- : Output parameter that receives an open relation if applicable
- : The lock mode to apply to the target object
- : If true, return invalid ObjectAddress instead of throwing error when object not found

## Dependencies
- Functions called/Symbols referenced:
  - [lcons](../l/lcons.md) (for list construction)
  - [makeString](../m/makeString.md) (for creating String nodes)
  - [get_object_address](get_object_address.md) (main object resolution function)
- Called from (representative examples):
  - [ExecAlterObjectDependsStmt](../E/ExecAlterObjectDependsStmt.md)

## Notes and Other Information
This function is essentially a helper that transforms RangeVar-based specifications into the list format expected by . It handles the common case where object names need to be qualified with relation information, making it easier for callers to work with relation-dependent objects. All other behavior, including locking and error handling, is identical to  since it delegates to that function after name construction.

## Simplified Source

```c
ObjectAddress
get_object_address_rv(ObjectType objtype, RangeVar *rel, List *object,
                      Relation *relp, LOCKMODE lockmode, bool missing_ok)
{
    // Build qualified object name by prepending relation components
    if (rel) {
        // Add relation name to front of object list
        object = lcons(makeString(rel->relname), object);

        // Add schema name if specified
        if (rel->schemaname)
            object = lcons(makeString(rel->schemaname), object);

        // Add catalog name if specified
        if (rel->catalogname)
            object = lcons(makeString(rel->catalogname), object);
    }

    // Delegate to main object address resolution function
    return get_object_address(objtype, (Node *) object, relp, lockmode, missing_ok);
}
```