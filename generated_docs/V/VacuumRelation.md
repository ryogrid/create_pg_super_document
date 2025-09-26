# VacuumRelation

## Location
src/include/nodes/parsenodes.h: 3852 - 3858

## Overview
VacuumRelation is a structure that represents information about a single target table for VACUUM/ANALYZE operations in PostgreSQL.

## Definition


## Detailed Description
VacuumRelation encapsulates the target information for VACUUM and ANALYZE commands. It can identify a table either by name (through RangeVar) or by OID. If the OID field is set, it always identifies the table to process. The relation field can be NULL in such cases; when present, it's used only to report failure to open/lock the relation. This dual identification mechanism provides flexibility in how tables are specified and helps with error reporting.

## Parameters / Member Variables
- : NodeTag identifying this as a VacuumRelation node
- : RangeVar pointer containing the table name to process, or NULL if identification is done by OID
- : Object identifier of the table; set to InvalidOid if not looked up yet
- : List of column names for ANALYZE operations, or NIL to analyze all columns

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar (for table name specification)
- Called from (representative examples):
  - ExecVacuum
  - vacuum
  - expand_vacuum_rel
  - makeVacuumRelation
  - autovacuum_do_vac_analyze

## Notes and Other Information
- This structure is part of the parse node hierarchy and inherits from Node
- Used in both manual VACUUM/ANALYZE commands and automatic vacuum operations
- The dual identification mechanism (name vs OID) allows for efficient table lookup while maintaining error reporting capabilities
- Column specification (va_cols) is only relevant for ANALYZE operations, not VACUUM