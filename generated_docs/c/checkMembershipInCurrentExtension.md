# checkMembershipInCurrentExtension

## Location
[src/backend/catalog/pg_depend.c:259-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L259-L301)

## Overview
Validates that an existing object is a member of the currently executing extension during CREATE IF NOT EXISTS operations, preventing security vulnerabilities from hostile object substitution.

## Definition

```c
void
checkMembershipInCurrentExtension(const ObjectAddress *object)
```
## Detailed Description
This function provides critical security validation for CREATE IF NOT EXISTS operations within extensions. When an extension uses CREATE IF NOT EXISTS and discovers that an object with the desired name already exists, this function ensures that the existing object is actually owned by the current extension. This prevents a serious security vulnerability where a hostile user could create objects with names that an extension might later try to create, potentially substituting malicious objects with arbitrary properties. The function only operates during CREATE EXTENSION operations and throws an error if the conflicting object is not owned by the current extension.

## Parameters / Member Variables
- : Pointer to ObjectAddress of the existing object to check for extension membership (must have objectSubId == 0)

## Dependencies
- Functions called/Symbols referenced:
  - [getExtensionOfObject](../g/getExtensionOfObject.md)
  - [getObjectDescription](../g/getObjectDescription.md)  
  - [get_extension_name](../g/get_extension_name.md)
- Called from (representative examples):
  - [CollationCreate](../C/CollationCreate.md)
  - [CreateTableAsRelExists](../C/CreateTableAsRelExists.md)
  - [CreateForeignServer](../C/CreateForeignServer.md)
  - [CreateSchemaCommand](../C/CreateSchemaCommand.md)
  - [DefineSequence](../D/DefineSequence.md)
  - [transformCreateStmt](../t/transformCreateStmt.md)

## Notes and Other Information
- Located in src/backend/catalog/pg_depend.c:259-301
- Only operates during CREATE EXTENSION operations (when creating_extension global is true)
- Only whole objects can be extension members (objectSubId must be 0)
- Critical security function preventing hostile object substitution attacks
- Specifically designed for CREATE IF NOT EXISTS scenarios where name conflicts exist
- Returns silently if the existing object is already owned by the current extension
- Throws detailed error with security explanation if object is not extension member
- Part of PostgreSQL's extension security model to ensure safe conditional object creation
- Similar logic to recordDependencyOnCurrentExtension but with different error messaging

## Simplified Source

```c
/*
 * Check that an object is a member of the current extension during
 * CREATE IF NOT EXISTS operations. This prevents security vulnerabilities
 * where hostile users could substitute objects with arbitrary properties.
 */
void
checkMembershipInCurrentExtension(const ObjectAddress *object)
{
    /* Only whole objects can be extension members */
    Assert(object->objectSubId == 0);

    if (creating_extension)
    {
        Oid oldext;

        oldext = getExtensionOfObject(object->classId, object->objectId);

        /* If already a member of this extension, OK */
        if (oldext == CurrentExtensionObject)
            return;

        /* Else complain */
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("%s is not a member of extension \"%s\"",
                        getObjectDescription(object, false),
                        get_extension_name(CurrentExtensionObject)),
                 errdetail("An extension may only use CREATE ... IF NOT EXISTS to skip object creation if the conflicting object is one that it already owns.")));
    }
}
```