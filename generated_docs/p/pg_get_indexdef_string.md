# pg_get_indexdef_string

## Location
[src/backend/utils/adt/ruleutils.c:1205-1214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1205-L1214)

## Overview
An internal C function that returns the complete CREATE INDEX statement including tablespace information, specifically designed for use by ALTER TABLE operations.

## Definition


## Detailed Description
This function provides an internal interface for retrieving index definitions that includes tablespace clauses, which are deliberately omitted from the SQL-callable versions. It's specifically designed for internal PostgreSQL operations like ALTER TABLE that need complete index recreation information. Unlike the SQL-callable functions, this returns a palloc'd C string without pretty-printing and includes all necessary information for reconstructing an index exactly as it was defined, including its tablespace assignment.

## Parameters / Member Variables
- : The OID of the index to retrieve the definition for

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that builds the index definition string
- Called from (representative examples):
  - : Used during table restructuring operations
  - Various ALTER TABLE operations that need to recreate indexes

## Notes and Other Information
- Internal function not exposed to SQL interface
- Includes tablespace clause unlike SQL-callable versions
- Returns palloc'd C string that caller must manage
- No pretty-printing applied (uses formatting flags = 0)
- Essential for ALTER TABLE operations that rebuild indexes
- Ensures complete index recreation with all original properties
- Part of PostgreSQL's rule utilities system for DDL reconstruction
- Located in src/backend/utils/adt/ruleutils.c:1205-1214
- Critical for maintaining index properties during table modifications