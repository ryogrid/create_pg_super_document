# init_uncommitted_enum_types

## Location
[src/backend/catalog/pg_enum.c:255-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L255-L271)

## Overview
Initializes the uncommitted_enum_types hash table to track enum types created within the current transaction.

## Definition

```c
static void
init_uncommitted_enum_types(void)
```
## Detailed Description
init_uncommitted_enum_types is a static initialization function that creates and configures the uncommitted_enum_types hash table used to track enum types created within the current transaction. This hash table is essential for PostgreSQL's enum handling because it enables the system to distinguish between enum types that were created in the current transaction versus those that existed before the transaction began.

The function sets up a hash table with OID keys and values, where each entry represents an enum type OID that was created in the current transaction. The hash table is allocated in TopTransactionContext to ensure it persists for the entire transaction lifetime and is automatically cleaned up when the transaction ends.

This tracking mechanism is crucial for operations like ALTER TYPE ADD VALUE, which have different behavior depending on whether the enum type was created in the current transaction or a previous one.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - HASH_ELEM (flag)
  - HASH_BLOBS (flag)
  - HASH_CONTEXT (flag)
- Called from:
  - [EnumValuesCreate](../E/EnumValuesCreate.md) (src/backend/catalog/pg_enum.c:106)
  - [RestoreUncommittedEnums](../R/RestoreUncommittedEnums.md) (src/backend/catalog/pg_enum.c:888)

## Notes and Other Information
- The function is declared static, making it internal to pg_enum.c
- Uses TopTransactionContext to ensure the hash table lifetime matches the transaction
- Initial hash table size is set to 32 entries, which will grow as needed
- HASH_BLOBS flag is used because OID keys are treated as opaque byte sequences
- The hash table uses OID as both key and entry data (keysize = entrysize = sizeof(Oid))
- This initialization is lazy - the hash table is only created when the first enum type is created in a transaction