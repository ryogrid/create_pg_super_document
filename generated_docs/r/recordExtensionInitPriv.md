# recordExtensionInitPriv

## Location
[src/backend/catalog/aclchk.c:4656-4684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4656-L4684)

## Overview
A static function that records initial ACL (Access Control List) for an extension object during extension creation or binary upgrade processes.

## Definition


## Detailed Description
This function serves as a wrapper that conditionally records initial privileges for extension objects. It only operates when either an extension is being created ( is set) or during binary upgrades when  is enabled. The function provides a mechanism to store the initial ACL state of objects that belong to extensions, which is essential for proper privilege management and restoration during database operations.

The function acts as a gatekeeper, checking the appropriate conditions before delegating the actual work to . This design allows the system to distinguish between normal privilege operations and those that should be recorded as initial extension privileges.

## Parameters / Member Variables
- : The OID of the object for which to record initial privileges
- : The OID of the system catalog table that defines the object type
- : Sub-object identifier (used for table columns, 0 for objects without sub-components)
- : The complete ACL to store; passing NULL removes existing entries for the object

## Dependencies
- Functions called/Symbols referenced:
  - [recordExtensionInitPrivWorker](recordExtensionInitPrivWorker.md)
  - Acl (data type)
- Called from (representative examples):
  - InternalDefaultACL
  - [ExecGrant_Attribute](../E/ExecGrant_Attribute.md)
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md)
  - [ExecGrant_common](../E/ExecGrant_common.md)
  - [ExecGrant_Largeobject](../E/ExecGrant_Largeobject.md)
  - [ExecGrant_Parameter](../E/ExecGrant_Parameter.md)

## Notes and Other Information
- The function is specifically designed to work during extension creation and binary upgrades with pg_upgrade
- It provides a clean interface for recording initial privileges without requiring callers to check the extension/upgrade context
- The function can replace existing ACL entries or remove them entirely by passing NULL
- Sub-object IDs are primarily used for table columns; other object types typically use 0
- The function is static, indicating it's only used within the aclchk.c compilation unit