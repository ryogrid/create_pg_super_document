# recordDependencyOnCurrentExtension

## Location
[src/backend/catalog/pg_depend.c:194-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L194-L258)

## Overview
Records a dependency between an object and the currently executing extension, ensuring proper extension membership during CREATE EXTENSION operations.

## Definition

```c
void
recordDependencyOnCurrentExtension(const ObjectAddress *object,
								   bool isReplace)
```
## Detailed Description
This function manages the relationship between database objects and PostgreSQL extensions during extension creation. It ensures that objects created within a CREATE EXTENSION context are properly marked as members of that extension. The function handles two scenarios: creation of new objects (isReplace=false) and replacement of existing objects (isReplace=true). For replacements, it performs strict validation to ensure security by preventing extensions from accidentally or maliciously taking ownership of free-standing objects. The function only operates when creating_extension is true, otherwise it does nothing.

## Parameters / Member Variables
- `*object`: Pointer to ObjectAddress of the object to be marked as extension member (must have objectSubId == 0)
- `isReplace`: Boolean indicating if the object already existed (true) or is newly created (false)
## Dependencies
- Functions called/Symbols referenced:
  - [getExtensionOfObject](../g/getExtensionOfObject.md)
  - [getObjectDescription](../g/getObjectDescription.md)
  - [get_extension_name](../g/get_extension_name.md)
  - [recordDependencyOn](recordDependencyOn.md)
  - DEPENDENCY_EXTENSION
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - [CastCreate](../C/CastCreate.md)
  - [CollationCreate](../C/CollationCreate.md)
  - [ConversionCreate](../C/ConversionCreate.md)
  - [NamespaceCreate](../N/NamespaceCreate.md)
  - [makeOperatorDependencies](../m/makeOperatorDependencies.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - [CreateAccessMethod](../C/CreateAccessMethod.md)
  - [CreateForeignDataWrapper](../C/CreateForeignDataWrapper.md)
  - [CreateTransform](../C/CreateTransform.md)

## Notes and Other Information
- Located in src/backend/catalog/pg_depend.c:194-258
- Only operates during CREATE EXTENSION operations (when creating_extension global is true)
- Only whole objects can be extension members (objectSubId must be 0)
- For isReplace=true, performs security checks to prevent unauthorized object takeover
- Rejects attempts to replace objects that are members of other extensions
- Rejects attempts to replace free-standing objects (not extension members)
- Creates DEPENDENCY_EXTENSION type dependency when recording the relationship
- Part of PostgreSQL's extension security model to maintain proper object ownership
- Used by virtually all object creation functions that support extension membership

## Simplified Source

```c
void recordDependencyOnCurrentExtension(const ObjectAddress *object, bool isReplace)
{
    // Only whole objects can be extension members
    Assert(object->objectSubId == 0);

    if (creating_extension) {
        ObjectAddress extension;

        // Only need to check for existing membership if isReplace
        if (isReplace) {
            Oid oldext;

            // Check if object is already a member of an extension
            oldext = getExtensionOfObject(object->classId, object->objectId);
            if (OidIsValid(oldext)) {
                // If already a member of this extension, nothing to do
                if (oldext == CurrentExtensionObject)
                    return;
                // Already a member of some other extension, so reject
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                         errmsg("%s is already a member of extension \"%s\"",
                                getObjectDescription(object, false),
                                get_extension_name(oldext))));
            }
            // It's a free-standing object, so reject
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("%s is not a member of extension \"%s\"",
                            getObjectDescription(object, false),
                            get_extension_name(CurrentExtensionObject)),
                     errdetail("An extension is not allowed to replace an object that it does not own.")));
        }

        // OK, record it as a member of CurrentExtensionObject
        extension.classId = ExtensionRelationId;
        extension.objectId = CurrentExtensionObject;
        extension.objectSubId = 0;

        recordDependencyOn(object, &extension, DEPENDENCY_EXTENSION);
    }
}
```