# checkDomainOwner

## Location
src/backend/commands/typecmds.c: 3490 - 3509

## Overview
Validates that a given type is actually a domain and that the current user has ownership privileges to perform ALTER DOMAIN operations on it.

## Definition
```c
void checkDomainOwner(HeapTuple tup)
```

## Detailed Description
This security validation function serves as a crucial gatekeeper for domain modification operations. It performs two essential checks: first, it verifies that the specified type is indeed a domain type (not a base type, composite type, or other type variant), and second, it confirms that the current user has ownership privileges required to alter the domain.

The function operates on a heap tuple from the pg_type catalog, extracting the type information and performing the necessary validations. If either check fails, it immediately raises an appropriate error with detailed information about the failure reason.

## Parameters / Member Variables
- `tup`: HeapTuple containing the pg_type catalog entry to be validated

## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT (extract struct from heap tuple)
  - format_type_be (format type name for error messages)
  - object_ownercheck (verify ownership privileges)
  - GetUserId (get current user ID)
  - aclcheck_error_type (report access control errors)
  - ereport (error reporting)
- Called from:
  - AlterDomainDefault (when changing domain default values)
  - AlterDomainNotNull (when modifying NOT NULL constraints)
  - AlterDomainAddConstraint (when adding check constraints)
  - AlterDomainDropConstraint (when removing constraints)
  - AlterDomainValidateConstraint (when validating constraints)
  - RenameConstraint (when renaming domain constraints)

## Notes and Other Information
- Essential security function that prevents unauthorized domain modifications
- Must be called before any ALTER DOMAIN operation to ensure proper authorization
- Raises ERRCODE_WRONG_OBJECT_TYPE if the object is not a domain
- Raises ACLCHECK_NOT_OWNER error if user lacks ownership privileges
- Part of the broader domain management security infrastructure
- Uses standard PostgreSQL access control mechanisms