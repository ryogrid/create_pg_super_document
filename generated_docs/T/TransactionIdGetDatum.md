# TransactionIdGetDatum

## Location
src/include/postgres.h: 272 - 281

## Overview
TransactionIdGetDatum is a static inline function that converts a transaction identifier (TransactionId) value to its Datum representation, serving as a type conversion utility for PostgreSQL's transaction management and function call interface.

## Definition
static inline Datum TransactionIdGetDatum(TransactionId X)

## Detailed Description
TransactionIdGetDatum performs a simple type cast from a TransactionId to a Datum. This function is the inverse of DatumGetTransactionId and is part of PostgreSQL's datum conversion interface. It provides a consistent method for converting typed TransactionId values into the generic Datum representation used throughout the PostgreSQL function call interface. This conversion is essential when PostgreSQL functions need to return transaction identifier values to client applications or store them in system catalogs. The function performs no validation or transformation - it simply casts the input TransactionId directly to a Datum type.

## Parameters / Member Variables
- X: A TransactionId value to be converted to Datum representation

## Dependencies
- Functions called/Symbols referenced:
  - (None - performs direct cast)
- Called from (representative examples):
  - heap_getsysattr (system attribute access in heap tuples)
  - pg_last_committed_xact (committed transaction information)
  - pg_prepared_xact (prepared transaction details)
  - createdb (database creation operations)
  - pg_lock_status (lock status reporting)
  - PG_STAT_GET_ACTIVITY_COLS (activity statistics)
  - PG_RETURN_TRANSACTIONID (function return macro)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it available throughout the codebase
- Extensively used in system functions that report transaction information to users
- Part of the family of *GetDatum conversion functions that provide type-safe conversion to Datum values
- Complementary to DatumGetTransactionId, forming a bidirectional conversion pair
- Essential for PostgreSQL's system information functions and monitoring capabilities
- Used heavily in control data reporting, replication functions, and lock status queries
- The function assumes the input TransactionId is valid - no validation is performed
- Critical for exposing internal transaction state to external applications and administrative tools