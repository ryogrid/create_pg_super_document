# buildoidvector

## Location
[src/backend/utils/adt/oid.c:87-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L87-L113)

## Overview
The buildoidvector function constructs an oidvector data structure from a raw array of OID values, creating PostgreSQL's specialized array type for storing sequences of object identifiers.

## Definition

```c
oidvector *
buildoidvector(const Oid *oids, int n)
```
## Detailed Description
The buildoidvector function creates an oidvector structure, which is PostgreSQL's specialized array type for storing OID sequences. It allocates memory for the structure, optionally copies the provided OID array, and initializes all necessary array header fields. The function supports creating empty oidvectors (when oids is NULL) that can be filled later, or fully populated ones from an existing OID array. This is commonly used in system catalog operations where sequences of OIDs need to be stored efficiently.

## Parameters / Member Variables
- : Pointer to a raw array of OID values to copy into the oidvector (can be NULL for empty initialization)
- : Number of OID elements in the array and size of the resulting oidvector

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md): PostgreSQL memory allocation function that zeros the allocated memory
  - OidVectorSize: Macro to calculate the total size needed for an oidvector with n elements
  - memcpy: Standard C library function to copy OID values from source array
  - SET_VARSIZE: Macro to set the variable-length header size for PostgreSQL variable-length types
- Called from (representative examples):
  - [StorePartitionKey](../S/StorePartitionKey.md): Used when storing partition key information in system catalogs
  - [UpdateIndexRelation](../U/UpdateIndexRelation.md): Used when updating index relation metadata
  - [DefineAggregate](../D/DefineAggregate.md): Used in aggregate function definition to store argument types
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md): Used in function definition to store parameter types
  - [makeRangeConstructors](../m/makeRangeConstructors.md): Used when creating constructor functions for range types
  - [makeMultirangeConstructors](../m/makeMultirangeConstructors.md): Used when creating constructor functions for multirange types

## Notes and Other Information
- The function creates an oidvector with standard PostgreSQL array header information
- Sets ndim=1 (one-dimensional array), dataoffset=0 (no nulls), elemtype=OIDOID
- Uses 0-based indexing (lbound1=0) for historical compatibility reasons
- Memory is allocated in the current memory context and will be freed when the context resets
- The oidvector type is optimized for system catalog storage and doesn't support null values
- Can create empty oidvectors for later population by passing NULL for oids parameter
- Location: src/backend/utils/adt/oid.c:87-113