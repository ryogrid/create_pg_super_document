# check_acl

## Location
src/backend/utils/adt/acl.c: 590 - 614

## Overview
Validates that an ACL (Access Control List) array conforms to expected structural requirements and data integrity constraints.

## Definition
```c
static void check_acl(const Acl *acl)
```

## Detailed Description
The check_acl function performs structural validation of an ACL array to ensure it meets PostgreSQL's internal requirements. It verifies three critical conditions: that the array contains elements of the correct ACLITEMOID type, that the array is one-dimensional (not multi-dimensional), and that the array contains no null values. If any of these conditions are violated, the function raises an appropriate error with specific error codes and messages. This validation is essential for maintaining data integrity in PostgreSQL's access control system.

## Parameters / Member Variables
- `acl`: Pointer to the ACL structure to validate

## Dependencies
- Functions called/Symbols referenced:
  - ARR_ELEMTYPE (macro to get array element type)
  - ARR_NDIM (macro to get array dimensions)
  - ARR_HASNULL (macro to check for null values)
  - ereport (PostgreSQL error reporting function)
  - ACLITEMOID (OID constant for ACL item type)
- Called from (representative examples):
  - aclupdate (src/backend/utils/adt/acl.c:1006)
  - aclnewowner (src/backend/utils/adt/acl.c:1133)
  - aclmask (src/backend/utils/adt/acl.c:1404)
  - aclcontains (src/backend/utils/adt/acl.c:1620)

## Notes and Other Information
- This is a static (internal) function used for validation within the ACL module
- Raises ERRCODE_INVALID_PARAMETER_VALUE for wrong data type or multi-dimensional arrays
- Raises ERRCODE_NULL_VALUE_NOT_ALLOWED for arrays containing null values
- Essential for maintaining ACL data integrity throughout the PostgreSQL system
- Called by most ACL manipulation functions to ensure input validity