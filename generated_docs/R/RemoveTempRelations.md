# RemoveTempRelations

## Location
[src/backend/catalog/namespace.c:4598-4623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4598-L4623)

## Overview
RemoveTempRelations removes all relations within a specified temporary namespace while preserving the namespace itself, used for cleanup during backend shutdown or when reusing pre-existing temporary namespaces.

## Definition

```c
static void
RemoveTempRelations(Oid tempNamespaceId)
```
## Detailed Description
This internal function performs a comprehensive cleanup of all database objects within a temporary namespace. It uses PostgreSQL's dependency deletion mechanism with specific flags to:

- Remove all relations and objects within the target namespace
- Preserve the namespace itself (SKIP_ORIGINAL flag)
- Perform cascading deletion of dependent objects
- Execute as an internal, quiet operation
- Skip deletion of extensions that might own temporary objects

The function is typically called in two scenarios:
1. During backend shutdown to clean up temporary relations created during the session
2. When beginning to use a pre-existing temporary namespace to remove objects left behind by crashed backends

## Parameters / Member Variables
- : The OID of the temporary namespace whose contents should be removed

## Dependencies
- Functions called/Symbols referenced:
  - [performDeletion](../p/performDeletion.md)
  - DROP_CASCADE
  - PERFORM_DELETION_INTERNAL
  - PERFORM_DELETION_QUIETLY
  - PERFORM_DELETION_SKIP_ORIGINAL
  - PERFORM_DELETION_SKIP_EXTENSIONS
- Called from (representative examples):
  - [InitTempTableNamespace](../I/InitTempTableNamespace.md)
  - [RemoveTempRelationsCallback](RemoveTempRelationsCallback.md)
  - [ResetTempTableNamespace](ResetTempTableNamespace.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the namespace.c file
- The function uses ObjectAddress structure to identify the target namespace for deletion
- The SKIP_ORIGINAL flag ensures the namespace container itself is not deleted, allowing it to be reused
- The SKIP_EXTENSIONS flag prevents accidental deletion of extensions that might own temporary objects
- Part of PostgreSQL's temporary object lifecycle management system