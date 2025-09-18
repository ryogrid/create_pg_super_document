# av_relation

## Location
src/backend/postmaster/autovacuum.c: 186 - 193

## Overview
The  structure is used by PostgreSQL autovacuum workers to track table-specific information during the first pass of determining which tables need vacuuming or analyzing.

## Definition


## Detailed Description
The  structure serves as a tracking mechanism for tables and their associated TOAST tables during autovacuum's table discovery and evaluation phase. It maintains the relationship between main tables and their TOAST tables, along with any table-specific autovacuum options that may have been configured. This structure is particularly important for managing TOAST table autovacuum operations, which have special handling requirements in PostgreSQL.

## Parameters / Member Variables
- : OID of the TOAST table associated with this relation (serves as hash key and must be first field)
- : OID of the main relation (table) that this entry represents
- : Boolean flag indicating whether this table has custom reloptions configured
- : Copy of the AutoVacOpts structure containing table-specific autovacuum configuration options from the main table's reloptions

## Dependencies
- Functions called/Symbols referenced:
  - AutoVacOpts (autovacuum options structure)
- Called from (representative examples):
  - do_autovacuum
  - table_recheck_autovac

## Notes and Other Information
- The  field must be positioned first to serve as a proper hash key
- This structure is used primarily during the first pass of autovacuum table evaluation
- Handles the special relationship between main tables and their TOAST tables
- The reloptions field allows for per-table customization of autovacuum behavior
- TOAST tables require special handling because they are automatically created and managed by PostgreSQL