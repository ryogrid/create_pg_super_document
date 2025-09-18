# RangeVarCallbackForPolicy

## Location
src/backend/commands/policy.c: 64 - 107

## Overview
A callback function used with RangeVarGetRelidExtended() to validate that a relation is suitable for row-level security policy operations by checking ownership, relation type, and system catalog restrictions.

## Definition


## Detailed Description
This function serves as a validation callback that is invoked during relation lookup operations for row-level security policy commands. It performs three critical security and type checks:

1. **Ownership verification**: Ensures the current user owns the target relation
2. **System catalog protection**: Prevents modifications to system catalogs unless explicitly allowed
3. **Relation type validation**: Confirms the target is a regular table or partitioned table

The function retrieves the relation's metadata from the system catalog and performs comprehensive validation before allowing policy operations to proceed. If any validation fails, it raises appropriate errors with specific error codes and messages.

## Parameters / Member Variables
- : Pointer to RangeVar structure containing the relation name and schema information
- : Object identifier of the resolved relation
- : Previous relation OID (used for rename operations, unused in this callback)
- : Generic argument pointer (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system catalog lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple data extraction)
  - object_ownercheck (ownership verification)
  - aclcheck_error (access control error reporting)
  - get_relkind_objtype (relation type description)
  - get_rel_relkind (relation kind retrieval)
  - IsSystemClass (system catalog detection)
  - ereport (error reporting)
  - ReleaseSysCache (cache cleanup)

- Called from:
  - CreatePolicy (policy creation operations)
  - AlterPolicy (policy modification operations)
  - rename_policy (policy rename operations)

## Notes and Other Information
- This is a static function, only accessible within the policy.c module
- The callback pattern allows for consistent validation across different policy operations
- Error handling provides specific error codes: ERRCODE_INSUFFICIENT_PRIVILEGE for ownership/system catalog issues, ERRCODE_WRONG_OBJECT_TYPE for invalid relation types
- Supports both regular tables (RELKIND_RELATION) and partitioned tables (RELKIND_PARTITIONED_TABLE) for policy operations
- System table modifications are controlled by the allowSystemTableMods global variable