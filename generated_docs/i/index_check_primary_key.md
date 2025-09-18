# index_check_primary_key

## Location
src/backend/catalog/index.c: 201 - 279

## Overview
Validates that a relation can have a primary key index created by checking for existing primary keys, ensuring columns are simple references (not expressions), and verifying all columns are marked NOT NULL.

## Definition


## Detailed Description
This function performs essential validation checks before creating a PRIMARY KEY index. It was originally part of DefineIndex() but was extracted to support ALTER TABLE ADD PRIMARY KEY USING INDEX operations. The function enforces several PostgreSQL constraints: (1) prevents creation of multiple primary keys on a table, (2) ensures primary key indexes don't use NULLS NOT DISTINCT, (3) validates that all indexed columns are simple column references rather than expressions, and (4) confirms all primary key columns are marked NOT NULL. The function expects the parser to have already inserted any required ALTER TABLE SET NOT NULL operations before attempting to create the primary key.

## Parameters / Member Variables
- : Relation pointer to the table where the primary key will be created (caller must hold at least ShareLock)
- : IndexInfo structure containing details about the index being created, including column information
- : Boolean flag indicating whether this is part of an ALTER TABLE operation
- : IndexStmt structure containing the index statement details (may be NULL in some contexts)

## Dependencies
- Functions called/Symbols referenced:
  - relationHasPrimaryKey: Checks if the relation already has a primary key
  - IndexInfo: Structure containing index metadata
  - IndexStmt: Structure representing index creation statement
  - SearchSysCache2: Searches system cache for attribute information
  - Int16GetDatum: Converts integer to PostgreSQL Datum format
  - RelationGetRelationName: Gets relation name for error messages
  - RelationGetRelid: Gets relation OID
  - HeapTupleIsValid: Validates heap tuple
  - Form_pg_attribute: PostgreSQL system catalog structure for attribute information
- Called from (representative examples):
  - DefineIndex: During index creation operations
  - ATExecAddIndexConstraint: During ALTER TABLE operations that add constraints

## Notes and Other Information
- The function performs different checks based on whether it's an ALTER TABLE operation or partition table creation
- System attributes (negative attnum) are automatically considered NOT NULL and skip validation
- NULLS NOT DISTINCT indexes cannot be used for primary keys due to uniqueness requirements
- Error handling provides detailed messages for different constraint violations
- The function assumes proper locking (ShareLock minimum) for reliable NOT NULL checking
- Historical behavior of automatically setting columns to NOT NULL was removed to avoid operation ordering issues in complex ALTER TABLE commands