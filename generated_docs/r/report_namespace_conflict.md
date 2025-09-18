# report_namespace_conflict

## Location
[src/backend/commands/alter.c:111-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L111-L164)

## Overview
A static helper function that raises an error indicating that an object with the given name already exists in a specific database namespace (schema).

## Definition
```c
static void report_namespace_conflict(Oid classId, const char *name, Oid nspOid)
```

## Detailed Description
This function generates appropriate error messages for duplicate object names within a specific namespace/schema based on the object class ID. It handles namespace-aware objects such as conversions, statistics objects, and various text search objects (parsers, dictionaries, templates, configurations). The function validates that the namespace OID is valid using Assert, then uses a switch statement to determine the correct error message format. Finally, it raises an ERROR with the ERRCODE_DUPLICATE_OBJECT error code and includes both the object name and schema name in the error message.

## Parameters / Member Variables
- `classId`: Object identifier (Oid) representing the class/type of the database object that has a naming conflict
- `name`: String containing the name of the conflicting object
- `nspOid`: Object identifier (Oid) of the namespace/schema where the conflict occurs

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for debugging assertions)
  - OidIsValid (for OID validation)
  - gettext_noop (for internationalization)
  - elog (for error logging)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md) (for error code specification)
  - [errmsg](../e/errmsg.md) (for error message formatting)
  - [get_namespace_name](../g/get_namespace_name.md) (to convert namespace OID to name)
  - ERRCODE_DUPLICATE_OBJECT (error code constant)

- Called from (representative examples):
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md) (src/backend/commands/alter.c:316)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md) (src/backend/commands/alter.c:789)

## Notes and Other Information
- This is a static function, only accessible within src/backend/commands/alter.c
- Specifically handles namespace-aware objects that can have naming conflicts within schemas
- Uses Assert macros for debugging validation of the namespace OID parameter
- The function supports text search objects (parsers, dictionaries, templates, configurations), conversions, and statistics objects
- Includes both object name and schema name in the error message for better user feedback
- Part of PostgreSQL's object renaming and namespace management subsystem