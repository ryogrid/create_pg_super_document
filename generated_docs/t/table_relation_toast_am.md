# table_relation_toast_am

## Location
[src/include/access/tableam.h:1888-1916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1888-L1916)

## Overview
Returns the OID of the access method (AM) that should be used to implement the TOAST table for a given relation.

## Definition


## Detailed Description
This function provides a table access method interface for determining which access method should be used to create and manage the TOAST table associated with a given relation. TOAST (The Oversized-Attribute Storage Technique) tables require their own access method, which may differ from the main table's access method depending on the storage engine's requirements and capabilities.

The function delegates to the underlying table access method's relation_toast_am function, allowing different storage engines to specify their preferred access method for TOAST table implementation. This is important because different access methods may have specific requirements or optimizations for handling large attribute storage.

## Parameters / Member Variables
- : A Relation pointer representing the main table relation for which the TOAST table access method is being determined

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_toast_am (table access method function pointer)
- Called from (representative examples):
  - [create_toast_table](../c/create_toast_table.md) (in src/backend/catalog/toasting.c:254)

## Notes and Other Information
- This is an inline function defined in the tableam header file for efficient access
- Part of the table access method abstraction layer that allows different storage engines
- Returns an OID that identifies the specific access method to be used for the TOAST table
- Different table access methods may choose different TOAST access methods based on their storage characteristics
- Critical for the TOAST table creation process during table definition and modification
- Located in src/include/access/tableam.h:1888-1916