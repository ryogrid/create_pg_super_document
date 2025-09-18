# ColumnIOData

## Location
src/backend/utils/adt/jsonfuncs.c: 210 - 226

## Overview
ColumnIOData is a structure that caches record metadata needed for populating record fields during JSON processing and record I/O operations in PostgreSQL.

## Definition


## Detailed Description
ColumnIOData serves as a comprehensive metadata cache structure for column type handling in PostgreSQL's JSON and record processing systems. It provides a unified interface for managing different types of columns by storing type identification information and specialized I/O data for each category of types. The structure uses a union to efficiently store type-specific metadata, supporting arrays, composite types, and domain types while also maintaining scalar I/O information for direct conversions.

## Parameters / Member Variables
- : OID of the column's data type
- : Type modifier providing additional type-specific information
- : TypeCat enumeration indicating the category of the column type (scalar, array, composite, domain, etc.)
- : ScalarIOData structure containing metadata cache for direct conversion through input functions
- : Union containing type-specific metadata caches:
  - : ArrayIOData for array type columns
  - : CompositeIOData for composite type columns  
  - : DomainIOData for domain type columns

## Dependencies
- Functions called/Symbols referenced:
  - TypeCat
  - ScalarIOData
  - ArrayIOData
  - CompositeIOData
  - DomainIOData
- Called from (representative examples):
  - ArrayIOData (nested reference)
  - DomainIOData (nested reference)
  - RecordIOData
  - PopulateRecordCache
  - JsObjectFree
  - prepare_column_cache
  - json_populate_type
  - populate_record_field
  - allocate_record_info
  - populate_record

## Notes and Other Information
- Defined in src/backend/utils/adt/jsonfuncs.c at lines 210-226
- Originally derived from hstore/record_out implementations according to source comments
- Central structure in PostgreSQL's JSON to record conversion system
- Also used extensively in general record I/O operations (rowtypes.c)
- The union design allows efficient memory usage while supporting all major PostgreSQL type categories
- Critical for performance optimization in record field population operations
- Provides abstraction layer that handles type complexity transparently