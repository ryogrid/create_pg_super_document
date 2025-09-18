# RecoveryPrefetchValue

## Location
src/include/access/xlogprefetcher.h: 29 - 31

## Overview
An enumeration type that defines the possible configuration values for the PostgreSQL recovery prefetch feature, controlling how the database prefetches referenced blocks during WAL replay recovery.

## Definition


## Detailed Description
 is an enumeration type that specifies the behavior of the recovery prefetching mechanism in PostgreSQL. This enum is used to control the  GUC (Grand Unified Configuration) parameter, which determines whether PostgreSQL should prefetch data pages that are referenced in the Write-Ahead Log (WAL) during recovery operations.

The recovery prefetch feature is designed to improve recovery performance by proactively reading data blocks that will be needed during WAL replay, reducing I/O latency by anticipating future data access patterns. This is particularly beneficial during crash recovery, archive recovery, and streaming replication scenarios.

## Parameters / Member Variables
- : Completely disables recovery prefetching functionality
- : Enables recovery prefetching unconditionally when maintenance_io_concurrency > 0
- : Enables recovery prefetching with error tolerance (default value) - attempts prefetching but continues gracefully if prefetch operations fail

## Dependencies
- Functions called/Symbols referenced:
  - XLogPrefetcher (related structure for prefetching implementation)
  
- Used by:
  - recovery_prefetch (GUC variable defined in xlogprefetcher.c:68)
  - recovery_prefetch_options (configuration enum entry table in guc_tables.c:381)
  - RecoveryPrefetchEnabled() macro (conditional compilation macro in xlogprefetcher.c:71-73)

## Notes and Other Information
- This enum is defined in 
- The default value is  as specified in the GUC configuration
- The enum values map to user-configurable string options: 'off', 'on', 'try' (plus boolean equivalents)
- Only available when PostgreSQL is compiled with  support
- The actual prefetching behavior also depends on the  setting
- Part of the PostgreSQL 15+ recovery prefetching infrastructure introduced to optimize recovery performance
- Configuration is changeable at runtime with SIGHUP signal (PGC_SIGHUP scope)
- Located in the WAL_RECOVERY configuration category