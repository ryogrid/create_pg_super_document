# hashvarlena

## Location
[src/backend/access/hash/hashfunc.c:383-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L383-L397)

## Overview
A general-purpose hash function for variable-length data types where all bits are significant for equality comparison.

## Definition

```c
struct varlena *key = PG_GETARG_VARLENA_PP(0);
```
## Detailed Description
The hashvarlena function provides hash computation for any variable-length (varlena) data type where there are no non-significant bits - meaning that distinct bit patterns never compare as equal. This function directly hashes the data content without any transformation, making it suitable for data types like bytea, varchar (in C locale), and other binary or simple text data where byte-for-byte comparison determines equality.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The varlena value to be hashed (struct varlena*)

## Dependencies
- Functions called/Symbols referenced:
  - [varlena](../v/varlena.md)
  - PG_GETARG_VARLENA_PP
  - [hash_any](hash_any.md)

- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Suitable only for data types where all bits are significant for equality
- Does not perform any collation or locale-specific transformations
- More efficient than text-specific hash functions for binary data
- Handles toasted inputs properly with PG_FREE_IF_COPY memory cleanup
- Located at src/backend/access/hash/hashfunc.c:383-397

## Simplified Source

```c
// hashvarlena() can be used for any varlena datatype in which there are
// no non-significant bits, ie, distinct bitpatterns never compare as equal.
Datum hashvarlena(PG_FUNCTION_ARGS) {
    struct varlena *key = PG_GETARG_VARLENA_PP(0);

    // Hash the variable-length data directly
    Datum result = hash_any((unsigned char *) VARDATA_ANY(key),
                           VARSIZE_ANY_EXHDR(key));

    // Clean up memory for toasted inputs
    PG_FREE_IF_COPY(key, 0);

    return result;
}
```