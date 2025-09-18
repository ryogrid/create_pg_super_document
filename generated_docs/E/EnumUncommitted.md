# EnumUncommitted

## Location
src/backend/catalog/pg_enum.c: 708 - 725

## Overview
Tests whether a given enum value OID is present in the table of uncommitted enum values, which tracks enum values created within the current transaction that haven't been committed yet.

## Definition


## Detailed Description
This function checks if a specific enum value (not the enum type itself, but an individual enum label/value) is considered "uncommitted" - meaning it was created in the current transaction and not yet committed. PostgreSQL maintains a separate hash table () to track such values during transaction processing. This is important for ensuring safe usage of enum values, as uncommitted enum values may not be visible to other transactions and have special handling requirements.

The function performs a simple hash table lookup and returns immediately if no uncommitted values table exists, optimizing for transactions that haven't created any new enum values.

## Parameters / Member Variables
- : Object identifier of the specific enum value to check for uncommitted status

## Dependencies
- Functions called/Symbols referenced:
  - hash_search: Performs hash table lookup to find the enum value ID
  - HASH_FIND: Hash operation flag indicating a search operation
  - uncommitted_enum_values: Global hash table tracking uncommitted enum values
- Called from (representative examples):
  - check_safe_enum_use: Validates whether enum values can be safely used in operations

## Notes and Other Information
- This is a public function (not static), making it accessible from other compilation units
- Differs from EnumTypeUncommitted by tracking individual enum values rather than enum types
- Returns false immediately if no uncommitted enum values table exists
- The uncommitted_enum_values hash table is managed globally and persists for the duration of a transaction
- Used to prevent unsafe operations on enum values that haven't been committed yet, ensuring transaction isolation
- Critical for maintaining consistency when enum values are created and used within the same transaction