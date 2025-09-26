# PgStatShared_Common

## Location
src/include/utils/pgstat_internal.h: 120 - 125

## Overview
PgStatShared_Common is the common header structure that must appear as the first element in all PostgreSQL shared statistics data structures.

## Definition


## Detailed Description
PgStatShared_Common serves as the standardized header for all shared statistics structures in PostgreSQL's statistics subsystem. This common header ensures that every shared statistics entry has consistent access control and validation mechanisms regardless of the specific statistics type (database, relation, function, etc.).

The magic field provides a basic integrity check to detect corruption or invalid memory access, while the LWLock provides fine-grained concurrency control for the statistics data that follows this header. Each statistics entry can be independently locked without affecting access to other statistics, enabling high concurrency in multi-backend environments.

This design pattern ensures that all specific shared statistics structures (PgStatShared_Database, PgStatShared_Relation, PgStatShared_Function, etc.) have consistent locking semantics and can be safely accessed through generic interfaces that expect this common header.

## Parameters / Member Variables
- : A magic number used for validity checking to detect memory corruption or invalid access
- : Lightweight lock (LWLock) that protects the statistics data that follows this header structure

## Dependencies
- Functions called/Symbols referenced:
  - LWLock (lightweight lock type)
- Called from (representative examples):
  - pgstat_init_entry (statistics entry initialization)
  - pgstat_acquire_entry_ref (entry reference acquisition)
  - pgstat_get_entry_ref (entry reference lookup)
  - pgstat_build_snapshot (snapshot building)
  - pgstat_get_entry_data (data access helper)
  - PgStatShared_Database (database statistics structure)
  - PgStatShared_Relation (relation statistics structure)
  - PgStatShared_Function (function statistics structure)

## Notes and Other Information
- This structure must be the first field in all PgStatShared_* structures to maintain binary compatibility and enable generic access patterns
- The lock protects only the statistics data that follows this header, not the header itself or the containing hashtable entry
- The magic field helps detect use-after-free or other memory corruption issues in the complex shared statistics system
- Used as a polymorphic base for all shared statistics types, enabling common operations across different statistics kinds
- The LWLock provides better performance than heavier synchronization primitives for the frequent statistics access patterns in PostgreSQL