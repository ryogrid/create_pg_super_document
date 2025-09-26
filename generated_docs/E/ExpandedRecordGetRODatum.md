# ExpandedRecordGetRODatum

## Location
src/include/utils/expandedrecord.h: 149 - 153

## Overview
Converts an ExpandedRecordHeader into a read-only Datum for PostgreSQL's function manager system.

## Definition

```c
typedef struct ExpandedRecordFieldInfo
{
	int			fnumber;		/* field's attr number in record */
	Oid			ftypeid;		/* field's type/typmod info */
	int32		ftypmod;
	Oid			fcollation;		/* field's collation if any */
} ExpandedRecordFieldInfo;
```
## Detailed Description
This inline function provides a convenient wrapper to convert an ExpandedRecordHeader pointer into a read-only Datum that can be used within PostgreSQL's function manager (fmgr) system. It delegates to the expanded object infrastructure by calling  on the header's embedded . The returned Datum represents a read-only reference to the expanded record, which prevents modifications while still allowing efficient access to the record's data without unnecessary conversions to flat tuple format.

## Parameters / Member Variables
- : Pointer to an ExpandedRecordHeader structure containing the expanded record data and metadata

## Dependencies
- Functions called/Symbols referenced:
  - EOHPGetRODatum
  - ExpandedRecordHeader
- Called from (representative examples):
  - expanded_record_set_fields
  - check_domain_for_new_field
  - check_domain_for_new_tuple

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Part of PostgreSQL's expanded object infrastructure for efficient handling of composite types
- The function provides read-only access, preventing modifications to the expanded record
- Used primarily when you need to pass expanded records as Datums but want to ensure immutability
- Commonly used in domain checking and field setting operations where read-only access is sufficient
- Located in src/include/utils/expandedrecord.h:149-153