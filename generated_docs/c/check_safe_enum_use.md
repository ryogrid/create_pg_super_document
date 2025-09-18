# check_safe_enum_use

## Location
src/backend/utils/adt/enum.c: 63 - 108

## Overview
Ensures that uncommitted enum values are not used in SQL operations to prevent index corruption during transaction rollbacks.

## Definition


## Detailed Description
This function implements a safety mechanism to prevent the use of uncommitted enum values in SQL operations. The primary concern is preventing index corruption that could occur if an uncommitted enum value gets into an index and the transaction is later rolled back. Since enum value comparisons rely on the underlying pg_enum catalog entry, removing the heap entry alone is insufficient to guarantee index integrity.

The function allows several exceptions:
- Values that are already committed (fast path via hint bits)
- Values belonging to enum types created in the same transaction
- Values created during CREATE TYPE AS ENUM operations
- Values added via ALTER TYPE ADD VALUE when the enum type was created in the current transaction

The implementation checks transaction states and maintains a list of uncommitted enum values to enforce these rules centrally.

## Parameters / Member Variables
- : HeapTuple representing the pg_enum catalog entry to validate for safe usage

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_enum
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderGetXmin
  - TransactionIdIsInProgress
  - TransactionIdDidCommit
  - EnumUncommitted
- Called from (representative examples):
  - enum_in
  - enum_recv
  - enum_endpoint
  - enum_range_internal

## Notes and Other Information
- This is a static function internal to enum.c, serving as a central validation point
- The function takes a conservative approach, being "stronger than necessary" to ensure safety
- Currently only handles ALTER TYPE ADD VALUE at the outermost transaction level
- Provides specific error messages with hints when unsafe usage is detected
- Critical for maintaining database integrity during concurrent enum modifications