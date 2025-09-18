# DatumGetChar

## Location
src/include/postgres.h: 112 - 121

## Overview
DatumGetChar is an inline function that extracts a character value from a PostgreSQL Datum by casting it to char type.

## Definition
static inline char DatumGetChar(Datum X)

## Detailed Description
DatumGetChar provides type conversion from PostgreSQL Datum format to a C char value. The function performs a simple cast operation, extracting the character value stored within the Datum. This is used throughout PostgreSQL when working with the char data type (single-byte character values). The function assumes the Datum contains a valid character representation and performs no validation or bounds checking. As an inline function, it provides efficient access to character values stored in the datum system.

## Parameters / Member Variables
- `X`: A Datum containing a character value to be extracted and returned as a C char type.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple type cast)
- Called from (representative examples):
  - AlterPolicy (src/backend/commands/policy.c:896)
  - fetch_remote_table_info (src/backend/replication/logical/tablesync.c:865, 867)
  - EnableDisableRule (src/backend/rewrite/rewriteDefine.c:728)
  - make_ruledef (src/backend/utils/adt/ruleutils.c:5187)
  - chareqfast (src/backend/utils/cache/catcache.c:193)
  - PG_GETARG_CHAR (src/include/fmgr.h:273)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h for maximum performance
- Used frequently in character data type operations and catalog cache functions
- The function performs no validation - assumes the Datum contains a valid character
- Part of PostgreSQLs fundamental datum conversion system for type-safe value access
- Commonly used in SQL function implementations and internal PostgreSQL operations handling char data