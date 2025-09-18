# AlterTypeNamespace_oid

## Location
[src/backend/commands/typecmds.c:4104-4155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L4104-L4155)

## Overview
A lower-level function that performs ALTER TYPE SET SCHEMA operations when the type and target schema OIDs are already resolved, with options for handling dependent types.

## Definition


## Detailed Description
AlterTypeNamespace_oid is an intermediate-level function that handles type namespace changes with pre-resolved OIDs. It performs ownership verification, prevents direct alteration of array types (directing users to alter the element type instead), and delegates the actual work to AlterTypeNamespaceInternal. The function includes a special mode for ignoring dependent types, which is useful when called from generic object namespace alteration routines.

## Parameters / Member Variables
- : OID of the type to be moved to a new namespace
- : OID of the target namespace where the type should be moved  
- : Boolean flag to silently ignore dependent types instead of raising errors
- : ObjectAddresses structure to track objects that have been moved during the operation

## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
  - [get_element_type](../g/get_element_type.md)
  - [get_array_type](../g/get_array_type.md)
  - [AlterTypeNamespaceInternal](AlterTypeNamespaceInternal.md)
- Called from (representative examples):
  - [AlterTypeNamespace](AlterTypeNamespace.md)
  - [AlterObjectNamespace_oid](AlterObjectNamespace_oid.md)

## Notes and Other Information
- Exported for use by AlterObjectNamespace_oid to handle dependent types gracefully
- Returns InvalidOid when ignoreDependent is true and the operation is skipped
- Prevents direct manipulation of array types, guiding users to alter the element type instead
- Returns the type's old namespace OID on successful completion, or InvalidOid if no action was taken
- Acts as a permission and validation layer before delegating to AlterTypeNamespaceInternal