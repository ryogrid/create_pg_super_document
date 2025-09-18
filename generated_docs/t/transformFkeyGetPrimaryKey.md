# transformFkeyGetPrimaryKey

## Location
src/backend/commands/tablecmds.c: 11945 - 12043

## Overview
transformFkeyGetPrimaryKey retrieves the names, attribute numbers, types, and index information for the primary key of a referenced table, used when the column list is omitted in a REFERENCES specification.

## Definition


## Detailed Description
This function automatically determines the primary key columns of a referenced table when creating a foreign key constraint without explicitly specifying the referenced columns. It searches through all indexes on the table to find the primary key index, validates that the primary key is suitable for foreign key references, and extracts detailed information about the primary key columns.

The function performs these key operations:
1. Searches all indexes on the referenced table to find the primary key
2. Validates that the primary key is immediate (not deferrable) per SQL specification
3. Extracts the index OID and operator classes from the primary key index
4. Builds lists of attribute numbers, column names, and type OIDs for all primary key columns
5. Returns the count of primary key columns

The function ensures the primary key is valid and immediate, as deferrable primary keys cannot be used as foreign key targets according to SQL standards.

## Parameters / Member Variables
- : The relation containing the primary key to examine
- : Output parameter for the OID of the primary key index
- : Output parameter for list of primary key column names
- : Output array for attribute numbers of primary key columns
- : Output array for type OIDs of primary key columns
- : Output array for operator class OIDs of primary key columns

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList
  - lfirst_oid
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
  - list_free
  - SysCacheGetAttrNotNull
  - DatumGetPointer
  - attnumTypeId
  - attnumAttName
  - makeString
  - pstrdup
  - lappend
- Called from (representative examples):
  - ATAddForeignKeyConstraint (when creating foreign keys without explicit column lists)

## Notes and Other Information
- Only works with immediate (non-deferrable) primary keys per SQL specification
- Assumes primary key indexes cannot contain expressional elements, only simple column references
- Returns the number of columns in the primary key
- All output parameters except pkrel must be provided by the caller
- Used specifically when the REFERENCES clause omits the column list, requiring automatic primary key detection
- Validates that the primary key index is both primary and valid before using it