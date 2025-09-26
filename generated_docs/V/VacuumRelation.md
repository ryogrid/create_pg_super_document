# VacuumRelation

## Location
[src/include/nodes/parsenodes.h:3852-3858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3852-L3858)

## Overview
VacuumRelation is a structure that represents information about a single target table for VACUUM/ANALYZE operations in PostgreSQL.

## Definition

```c
typedef struct VacuumRelation
{
	NodeTag		type;
	RangeVar   *relation;		/* table name to process, or NULL */
	Oid			oid;			/* table's OID; InvalidOid if not looked up */
	List	   *va_cols;		/* list of column names, or NIL for all */
} VacuumRelation;
```
## Detailed Description
VacuumRelation encapsulates the target information for VACUUM and ANALYZE commands. It can identify a table either by name (through RangeVar) or by OID. If the OID field is set, it always identifies the table to process. The relation field can be NULL in such cases; when present, it's used only to report failure to open/lock the relation. This dual identification mechanism provides flexibility in how tables are specified and helps with error reporting.

## Parameters / Member Variables
- : NodeTag identifying this as a VacuumRelation node
- : RangeVar pointer containing the table name to process, or NULL if identification is done by OID
- : Object identifier of the table; set to InvalidOid if not looked up yet
- : List of column names for ANALYZE operations, or NIL to analyze all columns

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](../R/RangeVar.md) (for table name specification)
- Called from (representative examples):
  - [ExecVacuum](../E/ExecVacuum.md)
  - [vacuum](../v/vacuum.md)
  - [expand_vacuum_rel](../e/expand_vacuum_rel.md)
  - [makeVacuumRelation](../m/makeVacuumRelation.md)
  - [autovacuum_do_vac_analyze](../a/autovacuum_do_vac_analyze.md)

## Notes and Other Information
- This structure is part of the parse node hierarchy and inherits from Node
- Used in both manual VACUUM/ANALYZE commands and automatic vacuum operations
- The dual identification mechanism (name vs OID) allows for efficient table lookup while maintaining error reporting capabilities
- Column specification (va_cols) is only relevant for ANALYZE operations, not VACUUM