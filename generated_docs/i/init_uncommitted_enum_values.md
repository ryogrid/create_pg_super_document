# init_uncommitted_enum_values

## Location
[src/backend/catalog/pg_enum.c:272-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L272-L291)

## Overview
Initializes the uncommitted_enum_values hash table to track individual enum values created within the current transaction.

## Definition


## Detailed Description
init_uncommitted_enum_values is a static initialization function that creates and configures the uncommitted_enum_values hash table used to track individual enum values added to existing enum types within the current transaction. This hash table works in conjunction with uncommitted_enum_types to provide complete transaction-level tracking of enum modifications.

The function sets up a hash table with OID keys and values, where each entry represents an enum value OID that was added via ALTER TYPE ADD VALUE in the current transaction. This tracking is essential for PostgreSQL's enum constraint enforcement and ordering logic, particularly for operations that need to distinguish between enum values that existed before the transaction and those added during it.

The hash table is allocated in TopTransactionContext to ensure proper transaction lifecycle management and automatic cleanup when the transaction completes.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - HASH_ELEM (flag)
  - HASH_BLOBS (flag) 
  - HASH_CONTEXT (flag)
- Called from:
  - [AddEnumLabel](../A/AddEnumLabel.md) (src/backend/catalog/pg_enum.c:595)
  - [RestoreUncommittedEnums](../R/RestoreUncommittedEnums.md) (src/backend/catalog/pg_enum.c:899)

## Notes and Other Information
- The function is declared static, making it internal to pg_enum.c
- Uses TopTransactionContext to ensure the hash table lifetime matches the transaction
- Initial hash table size is set to 32 entries, with automatic expansion as needed
- HASH_BLOBS flag treats OID keys as opaque byte sequences for efficient comparison
- The hash table structure mirrors uncommitted_enum_types but tracks individual enum values rather than enum types
- This initialization is lazy - the hash table is only created when the first enum value is added to an existing enum type in a transaction
- Critical for maintaining enum value ordering constraints and transaction isolation