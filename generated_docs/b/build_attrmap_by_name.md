# build_attrmap_by_name

## Location
src/backend/access/common/attmap.c: 177 - 262

## Overview
Builds an attribute map for tuple conversion by matching input and output columns by their names rather than positions, with optimized searching and optional tolerance for missing columns.

## Definition
```c
AttrMap *build_attrmap_by_name(TupleDesc indesc, TupleDesc outdesc, bool missing_ok)
```

## Detailed Description
The `build_attrmap_by_name` function creates an attribute map by matching columns between tuple descriptors based on column names rather than positions. This is particularly useful when column order might differ between source and target structures. The function implements an optimized search algorithm that assumes partitioned tables likely have attributes in similar order, starting each search from where the previous match was found.

The function iterates through each non-dropped column in the output descriptor, searches for a matching column name in the input descriptor, validates type compatibility, and builds the mapping. Unlike position-based mapping, this approach is more flexible when dealing with column reordering but requires exact name matching.

## Parameters / Member Variables
- `indesc`: Input tuple descriptor containing source columns
- `outdesc`: Output tuple descriptor containing target columns  
- `missing_ok`: If true, missing columns in input are tolerated (mapped to 0); if false, missing columns cause an error

## Dependencies
- Functions called/Symbols referenced:
  - `make_attrmap` (creates the basic attribute map structure)
  - `TupleDescAttr` (accesses tuple descriptor attributes)
  - `NameStr` (extracts name from pg_attribute)
  - `strcmp` (string comparison for name matching)
  - `format_type_be` (formats type names for error messages)
  - `ereport` (error reporting)
- Called from (representative examples):
  - `build_attrmap_by_name_if_req`
  - `map_partition_varattnos`
  - `DefineIndex`
  - `DefineRelation`
  - `ExecInitPartitionInfo`
  - `expandTableLikeClause`

## Notes and Other Information
- Uses an optimized circular search algorithm starting from the last successful match position
- Dropped columns are ignored in both input and output descriptors
- Requires exact type and typemod matching - no implicit conversions
- The `missing_ok` parameter provides flexibility for scenarios where not all output columns need corresponding input columns
- Commonly used in partitioning, table inheritance, and schema evolution scenarios
- Search optimization assumes similar column ordering between related tables (e.g., partitioned tables)
- Located in `src/backend/access/common/attmap.c:177-262`