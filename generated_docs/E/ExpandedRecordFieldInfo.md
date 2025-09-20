# ExpandedRecordFieldInfo

## Location
[src/include/utils/expandedrecord.h:168-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/expandedrecord.h#L168-L174)

## Overview
ExpandedRecordFieldInfo is a structure that holds metadata information about a specific field in an expanded record, returned by the expanded_record_lookup_field() function to provide type and attribute details for record field access.

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
ExpandedRecordFieldInfo serves as a container for essential metadata about a field within an expanded record structure. This structure is specifically designed to be populated by the expanded_record_lookup_field() function when looking up fields by name in expanded records. It encapsulates all the critical type system information needed to properly handle field values, including the field's position, data type, type modifier, and collation information.

The structure is part of PostgreSQL's expanded object system, which provides an optimized representation for composite types that allows efficient field access and modification without repeated tuple construction/deconstruction. When a field is found during lookup, this structure provides all necessary metadata to subsequently fetch, validate, or modify the field's value.

## Parameters / Member Variables
- `fnumber`: The attribute number of the field within the record's tuple descriptor. This corresponds to the field's position and is used as an index for field access operations
- `ftypeid`: The OID of the field's data type from the pg_type system catalog, identifying what kind of data this field contains
- `ftypmod`: The type modifier providing additional type-specific information (e.g., precision for numeric types, length for varchar)
- `fcollation`: The OID of the collation rule to be used for this field if it contains collatable data (text types), or InvalidOid if not applicable
## Dependencies
- Functions called/Symbols referenced:
  - Oid (from PostgreSQL type system)
  - int32 (standard PostgreSQL type alias)
- Called from (representative examples):
  - [expanded_record_lookup_field](../e/expanded_record_lookup_field.md)

## Notes and Other Information
- This structure is specifically designed as an output parameter for field lookup operations rather than for direct instantiation
- The structure contains only metadata and does not hold the actual field value
- All OID fields reference entries in PostgreSQL's system catalogs (pg_type, pg_collation)
- The fnumber field uses PostgreSQL's standard attribute numbering system where user attributes start at 1 and system attributes have negative numbers
- This structure is part of the expanded record infrastructure introduced to optimize composite type handling in PostgreSQL
- The information in this structure corresponds directly to attributes found in tuple descriptors (TupleDesc structures)