# pg_column_is_updatable

## Location
[src/backend/utils/adt/misc.c:665-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L665-L680)

## Overview
A PostgreSQL system function that determines whether a specific column in a table is updatable, encapsulating the logic for the SQL standard information_schema.columns.is_updatable functionality.

## Definition


## Detailed Description
The  function determines whether a column can be updated based on PostgreSQL's rules for updatable views and tables. This function specifically implements the logic behind the SQL standard  column, providing a centralized decision point that can be modified without requiring database reinitialization.

The function performs several checks: it immediately returns false for system columns (those with attribute numbers <= 0), then uses the  function to determine if the column's relation supports both UPDATE and DELETE operations. The requirement for both operations reflects PostgreSQL's interpretation of what makes a column truly "updatable" in the context of the SQL standard.

## Parameters / Member Variables
-  (Oid): The OID of the relation (table/view) containing the column, obtained via 
-  (AttrNumber): The attribute number of the column to check, obtained via 
-  (bool): Whether to include trigger-based updatability in the assessment, obtained via 

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the relation OID argument
  - : Extracts the attribute number argument  
  - : Extracts the include_triggers boolean argument
  - : Constant used for attribute number calculation
  - : Core function that determines relation updatability
  - : Creates a bitmap set containing a single column
  - : Returns boolean result to the SQL engine
  - : Command type constant for UPDATE operations
  - : Command type constant for DELETE operations

- Called from (representative examples):
  - Information schema views
  - SQL queries checking column updatability
  - PostgreSQL system catalog functions

## Notes and Other Information
- System columns (attribute numbers <= 0) are never considered updatable
- The function requires both UPDATE and DELETE capabilities for a column to be considered updatable (REQ_EVENTS mask)
- The  variable is calculated as  to convert to zero-based indexing
- The design allows for future changes to updatability logic without requiring initdb
- This function bridges PostgreSQL's internal updatability logic with SQL standard requirements
- Located in  at lines 665-680