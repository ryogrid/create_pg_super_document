# EnumTypeUncommitted

## Location
src/backend/catalog/pg_enum.c: 690 - 707

## Overview
Tests whether a given enum type OID is present in the table of uncommitted enum types, which tracks enum types created within the current transaction that haven't been committed yet.

## Definition


## Detailed Description
This is a utility function that checks if an enum type is considered "uncommitted" - meaning it was created in the current transaction and not yet committed. PostgreSQL maintains a hash table () to track such types during transaction processing. This information is crucial for determining whether certain operations on enum types are allowed, as some operations have different behavior or restrictions when applied to uncommitted enum types.

The function uses a simple hash table lookup to determine membership, returning immediately if no uncommitted types table exists (indicating no enum types have been created in the current transaction).

## Parameters / Member Variables
- : Object identifier of the enum type to check for uncommitted status

## Dependencies
- Functions called/Symbols referenced:
  - hash_search: Performs hash table lookup to find the type ID
  - HASH_FIND: Hash operation flag indicating a search operation
  - uncommitted_enum_types: Global hash table tracking uncommitted enum types
- Called from (representative examples):
  - AddEnumLabel: Checks if enum type is uncommitted before adding new labels

## Notes and Other Information
- This is a static function, only accessible within the pg_enum.c compilation unit
- Returns false immediately if no uncommitted enum types table exists, optimizing for the common case
- The uncommitted_enum_types hash table is managed globally and persists for the duration of a transaction
- Used primarily to enforce restrictions on enum modifications within the same transaction that created the enum type
- The function is read-only and does not modify the uncommitted types table