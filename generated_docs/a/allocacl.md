# allocacl

## Location
[src/backend/utils/adt/acl.c:426-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L426-L447)

## Overview
Allocates memory for a new Access Control List (ACL) with a specified number of entries, properly initializing the array structure metadata.

## Definition

```c
static Acl *
allocacl(int n)
```
## Detailed Description
This function creates a new ACL data structure by allocating memory and initializing all the required array metadata. ACLs in PostgreSQL are implemented as variable-length arrays (varlena) containing AclItem structures. The function calculates the appropriate size based on the number of entries, allocates zero-initialized memory using palloc0, and sets up the array dimensions and properties.

The function initializes the ACL as a one-dimensional array with no null values, sets the element type to ACLITEMOID, and configures the array bounds to start at index 1 (following PostgreSQL's array indexing convention). Input validation ensures that negative sizes are rejected with an error.

## Parameters / Member Variables
- `n`: Number of AclItem entries to allocate space for in the new ACL
## Dependencies
- Functions called/Symbols referenced:
  - ACL_N_SIZE (macro to calculate total size needed for n ACL items)
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - SET_VARSIZE (macro to set the varlena header size)
  - ARR_LBOUND (macro to access array lower bound)
  - ARR_DIMS (macro to access array dimensions)
  - ACLITEMOID (OID constant for AclItem type)
  - elog (error logging function)
- Called from (representative examples):
  - [make_empty_acl](../m/make_empty_acl.md) (creates empty ACLs)
  - [aclcopy](aclcopy.md) (duplicates existing ACLs)
  - [aclconcat](aclconcat.md) (combines multiple ACLs)
  - [acldefault](acldefault.md) (creates default ACLs)
  - [aclupdate](aclupdate.md) (modifies existing ACLs)
  - [aclnewowner](aclnewowner.md) (updates ACL ownership)

## Notes and Other Information
The function always creates ACLs with dataoffset=0 since ACL arrays never contain null values, and ndim=1 since ACLs are always one-dimensional arrays. The lower bound is set to 1 following PostgreSQL's standard array indexing convention. This is a fundamental utility function used throughout the ACL system for creating new ACL structures.

## Simplified Source

```c
static Acl *
allocacl(int n)
{
    Acl    *new_acl;
    Size    size;

    if (n < 0)
        elog(ERROR, "invalid size: %d", n);

    size = ACL_N_SIZE(n);                    // Calculate memory needed
    new_acl = (Acl *) palloc0(size);        // Allocate zero-filled memory
    SET_VARSIZE(new_acl, size);              // Set varlena size header

    // Initialize array metadata
    new_acl->ndim = 1;                       // One-dimensional array
    new_acl->dataoffset = 0;                 // No nulls in ACL arrays
    new_acl->elemtype = ACLITEMOID;          // Element type is AclItem
    ARR_LBOUND(new_acl)[0] = 1;             // Array starts at index 1
    ARR_DIMS(new_acl)[0] = n;               // Array has n elements

    return new_acl;
}
```

**Simplified Explanation:**
1. Validate that the requested size is not negative
2. Calculate the total memory size needed for n ACL items
3. Allocate zero-initialized memory using palloc0
4. Set up the PostgreSQL varlena header with the size
5. Initialize array metadata (1D array, no nulls, proper bounds)
6. Return the initialized ACL structure