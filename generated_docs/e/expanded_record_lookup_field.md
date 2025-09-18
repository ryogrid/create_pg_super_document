# expanded_record_lookup_field

## Location
src/backend/utils/adt/expandedrecord.c: 1017 - 1062

## Overview
Searches for a field by name within an expanded record and returns metadata information about the field if found.

## Definition


## Detailed Description
This function performs a field lookup operation on an expanded record using a field name string. It searches through both user-defined attributes and system attributes to find a matching field. The function uses a two-phase search strategy:

1. First, it iterates through all user-defined attributes in the tuple descriptor, comparing the attribute name with the requested fieldname using namestrcmp. It skips dropped attributes during this search.

2. If no user-defined attribute matches, it checks system attributes using SystemAttributeByName, which handles special system columns like oid, tableoid, xmin, xmax, etc.

When a matching field is found, the function populates the ExpandedRecordFieldInfo structure with the field's metadata (attribute number, type OID, type modifier, and collation) and returns true. If no field is found, it returns false without modifying the finfo structure.

## Parameters / Member Variables
- : Pointer to the ExpandedRecordHeader containing the record to search
- : Name of the field to look up (null-terminated string)
- : Output parameter - pointer to ExpandedRecordFieldInfo structure to be filled with field metadata

## Dependencies
- Functions called/Symbols referenced:
  - expanded_record_get_tupdesc
  - TupleDescAttr (macro)
  - namestrcmp
  - [SystemAttributeByName](../S/SystemAttributeByName.md)
- Types referenced:
  - ExpandedRecordHeader
  - [ExpandedRecordFieldInfo](../E/ExpandedRecordFieldInfo.md)
  - [TupleDesc](../T/TupleDesc.md)
  - Form_pg_attribute
  - FormData_pg_attribute
- Called from (representative examples):
  - Currently no direct callers found in the codebase

## Notes and Other Information
- The function supports both user-defined and system attributes
- System attributes include special columns like oid, tableoid, xmin, xmax, cmin, cmax, ctid
- Dropped attributes are automatically skipped during the search
- The function is case-sensitive for field name matching
- Returns false if the field doesn't exist, making it safe for conditional field access
- The ExpandedRecordFieldInfo structure contains: fnumber (attribute number), ftypeid (type OID), ftypmod (type modifier), and fcollation (collation OID)