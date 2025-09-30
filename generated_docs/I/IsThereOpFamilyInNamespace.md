# IsThereOpFamilyInNamespace

## Location
[src/backend/commands/opclasscmds.c:1828-1842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1828-L1842)

## Overview
Validates that an operator family with the specified name and access method does not already exist in a given namespace, raising an error if a duplicate is found.

## Definition

```c
void
IsThereOpFamilyInNamespace(const char *opfname, Oid opfmethod,
						   Oid opfnamespace)
```
## Detailed Description
This function serves as a validation subroutine used during ALTER OPERATOR FAMILY operations, specifically for SET SCHEMA and RENAME operations. It performs a uniqueness check by searching the system catalogs to determine if an operator family with the given name and access method already exists in the target namespace.

The function uses the system cache (OPFAMILYAMNAMENSP) to efficiently lookup existing operator families. If a duplicate is found, it immediately raises an ERROR with code ERRCODE_DUPLICATE_OBJECT, providing a detailed error message that includes the operator family name, access method name, and schema name.

This validation prevents naming conflicts and maintains the integrity of the operator family namespace organization within PostgreSQL's type system.

## Parameters / Member Variables
- : The name of the operator family to check for existence
- : The OID of the access method associated with the operator family
- : The OID of the namespace (schema) where the existence check is performed

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists3 (system cache lookup function)
  - [CStringGetDatum](../C/CStringGetDatum.md) (datum conversion utility)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (datum conversion utility)
  - [get_am_name](../g/get_am_name.md) (retrieves access method name for error reporting)
  - [get_namespace_name](../g/get_namespace_name.md) (retrieves schema name for error reporting)
  - ereport (error reporting function)
- Called from (representative examples):
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md) (when renaming operator families)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md) (when moving operator families to different schemas)

## Notes and Other Information
- This function is specifically designed for ALTER OPERATOR FAMILY operations and acts as a prerequisite validation step
- The function uses a 3-parameter system cache lookup (OPFAMILYAMNAMENSP) which indexes on access method OID, family name, and namespace OID
- Error messages are user-friendly and include all relevant identifying information (family name, access method name, and schema name)
- The function follows PostgreSQL's pattern of immediate error reporting rather than returning boolean status values
- Located in src/backend/commands/opclasscmds.c:1828-1842

## Simplified Source

```c
void IsThereOpFamilyInNamespace(const char *opfname, Oid opfmethod, Oid opfnamespace)
{
    // Check if operator family already exists with same name and access method
    if (SearchSysCacheExists3(OPFAMILYAMNAMENSP,
                             ObjectIdGetDatum(opfmethod),
                             CStringGetDatum(opfname),
                             ObjectIdGetDatum(opfnamespace)))
    {
        // Report comprehensive error with family name, access method, and schema
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("operator family \"%s\" for access method \"%s\" already exists in schema \"%s\"",
                        opfname,
                        get_am_name(opfmethod),
                        get_namespace_name(opfnamespace))));
    }
}
```