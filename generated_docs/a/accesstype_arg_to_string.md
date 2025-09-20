# accesstype_arg_to_string

## Location
[src/test/modules/test_oat_hooks/test_oat_hooks.c:458-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_oat_hooks/test_oat_hooks.c#L458-L518)

## Overview
Converts object access type-specific argument structures to human-readable strings for audit logging and error reporting in PostgreSQL's object access hook testing framework.

## Definition

```c
static char *
accesstype_arg_to_string(ObjectAccessType access, void *arg)
```
## Detailed Description
This function is part of PostgreSQL's test module for object access hooks (). It takes an  enumeration value and its corresponding argument structure, then generates a descriptive string representation of the access-specific information.

The function handles different types of object access operations by casting the generic  parameter to the appropriate structure type based on the access type. This enables detailed logging and debugging of object access hook behavior during testing.

The function supports the following object access types:
- **OAT_POST_CREATE**: Reports whether object creation was internal or explicit
- **OAT_DROP**: Provides detailed information about drop operation flags
- **OAT_POST_ALTER**: Describes alter operations with auxiliary object information
- **OAT_NAMESPACE_SEARCH**: Reports namespace search behavior and results
- **OAT_TRUNCATE/OAT_FUNCTION_EXECUTE**: These access types don't expect arguments

## Parameters / Member Variables
- : The type of object access operation (ObjectAccessType enum value)
- : Generic pointer to access-type-specific argument structure (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md)
  - [psprintf](../p/psprintf.md)
  - OidIsValid
  - [ObjectAccessType](../O/ObjectAccessType.md) (enum)
  - [ObjectAccessPostCreate](../O/ObjectAccessPostCreate.md) (struct)
  - [ObjectAccessDrop](../O/ObjectAccessDrop.md) (struct)
  - [ObjectAccessPostAlter](../O/ObjectAccessPostAlter.md) (struct)
  - [ObjectAccessNamespaceSearch](../O/ObjectAccessNamespaceSearch.md) (struct)
  - PERFORM_DELETION_* flags
  - OAT_* enumeration values

- Called from (representative examples):
  - [REGRESS_object_access_hook](../R/REGRESS_object_access_hook.md) (lines 329, 336, 344)

## Notes and Other Information
- This is a static function used exclusively within the test_oat_hooks testing module
- The function handles NULL arguments gracefully by returning "extra info null"
- For drop operations, it constructs a comma-separated list of active deletion flags
- Returns appropriate error messages for unexpected cases (unrecognized access types or unexpected arguments)
- Memory for returned strings is allocated using PostgreSQL's memory management functions (pstrdup/psprintf)
- Used primarily for audit logging and error reporting in object access hook testing scenarios
- Location: src/test/modules/test_oat_hooks/test_oat_hooks.c:458-518