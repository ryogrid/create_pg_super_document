# ColumnIOData

## Location
[src/backend/utils/adt/jsonfuncs.c:210-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L210-L226)

## Overview
ColumnIOData is a structure that caches record metadata needed for populating record fields during JSON processing and record I/O operations in PostgreSQL.

## Definition

```c
struct ColumnIOData
{
	Oid			typid;			/* column type id */
	int32		typmod;			/* column type modifier */
	TypeCat		typcat;			/* column type category */
	ScalarIOData scalar_io;		/* metadata cache for direct conversion
								 * through input function */
	union
	{
		ArrayIOData array;
		CompositeIOData composite;
		DomainIOData domain;
	}			io;				/* metadata cache for various column type
								 * categories */
};
```
## Detailed Description
ColumnIOData serves as a comprehensive metadata cache structure for column type handling in PostgreSQL's JSON and record processing systems. It provides a unified interface for managing different types of columns by storing type identification information and specialized I/O data for each category of types. The structure uses a union to efficiently store type-specific metadata, supporting arrays, composite types, and domain types while also maintaining scalar I/O information for direct conversions.

## Parameters / Member Variables
- `typid`: OID of the column's data type
- `typmod`: Type modifier providing additional type-specific information
- `typcat`: TypeCat enumeration indicating the category of the column type (scalar, array, composite, domain, etc.)
- `scalar_io`: ScalarIOData structure containing metadata cache for direct conversion through input functions
- `io`: Union containing type-specific metadata caches:
  - `array`: ArrayIOData for array type columns
  - `composite`: CompositeIOData for composite type columns  
  - `domain`: DomainIOData for domain type columns

## Dependencies
- Functions called/Symbols referenced:
  - TypeCat
  - [ScalarIOData](../S/ScalarIOData.md)
  - [ArrayIOData](../A/ArrayIOData.md)
  - [CompositeIOData](CompositeIOData.md)
  - [DomainIOData](../D/DomainIOData.md)
- Called from (representative examples):
  - [ArrayIOData](../A/ArrayIOData.md) (nested reference)
  - [DomainIOData](../D/DomainIOData.md) (nested reference)
  - [RecordIOData](../R/RecordIOData.md)
  - [PopulateRecordCache](../P/PopulateRecordCache.md)
  - JsObjectFree
  - [prepare_column_cache](../p/prepare_column_cache.md)
  - [json_populate_type](../j/json_populate_type.md)
  - [populate_record_field](../p/populate_record_field.md)
  - [allocate_record_info](../a/allocate_record_info.md)
  - [populate_record](../p/populate_record.md)

## Notes and Other Information
- Defined in src/backend/utils/adt/jsonfuncs.c at lines 210-226
- Originally derived from hstore/record_out implementations according to source comments
- Central structure in PostgreSQL's JSON to record conversion system
- Also used extensively in general record I/O operations (rowtypes.c)
- The union design allows efficient memory usage while supporting all major PostgreSQL type categories
- Critical for performance optimization in record field population operations
- Provides abstraction layer that handles type complexity transparently