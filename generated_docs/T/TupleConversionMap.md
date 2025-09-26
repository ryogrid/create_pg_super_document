# TupleConversionMap

## Location
[src/include/access/tupconvert.h:24-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tupconvert.h#L24-L33)

## Overview
TupleConversionMap is a data structure that facilitates conversion between tuple formats with different column layouts, supporting scenarios where logically equivalent rowtypes have columns in different orders or different sets of dropped columns.

## Definition

```c
typedef struct TupleConversionMap
{
	TupleDesc	indesc;			/* tupdesc for source rowtype */
	TupleDesc	outdesc;		/* tupdesc for result rowtype */
	AttrMap    *attrMap;		/* indexes of input fields, or 0 for null */
	Datum	   *invalues;		/* workspace for deconstructing source */
	bool	   *inisnull;
	Datum	   *outvalues;		/* workspace for constructing result */
	bool	   *outisnull;
} TupleConversionMap;
```
## Detailed Description
TupleConversionMap provides the infrastructure for converting tuples between logically compatible but physically different row types. This is commonly needed in inheritance relationships, partitioning, triggers, and other scenarios where data needs to be mapped between relations with similar but not identical schemas.

The structure contains both metadata (tuple descriptors and attribute mapping) and preallocated workspace arrays to efficiently perform the conversion process. The conversion can handle:
- Different column ordering between source and destination
- Dropped columns in either relation
- Type compatibility verification
- NULL value handling for missing or dropped columns

The conversion process works by using the AttrMap to determine how to map each output column to the corresponding input column (or to NULL for dropped columns), then using the workspace arrays to deconstruct the input tuple and reconstruct the output tuple.

## Parameters / Member Variables
- `indesc`: TupleDesc describing the structure of the source/input tuple format
- `outdesc`: TupleDesc describing the structure of the target/output tuple format
- `*attrMap`: Pointer to AttrMap structure containing the mapping between input and output column numbers, with 0 entries indicating NULL values for dropped columns
- `*invalues`: Preallocated Datum array workspace used for deconstructing the source tuple values (size: indesc->natts + 1)
- `*inisnull`: Preallocated boolean array workspace indicating NULL status of source tuple values
- `*outvalues`: Preallocated Datum array workspace used for constructing the result tuple values (size: outdesc->natts + 1)
- `*outisnull`: Preallocated boolean array workspace indicating NULL status of result tuple values
## Dependencies
- Functions called/Symbols referenced:
  - [AttrMap](../A/AttrMap.md) (attribute mapping structure)
  - [TupleDesc](TupleDesc.md) (tuple descriptor type)
  - Datum (PostgreSQL data value type)

- Called from (representative examples):
  - [convert_tuples_by_position](../c/convert_tuples_by_position.md) (creates conversion maps based on column position)
  - [convert_tuples_by_name](../c/convert_tuples_by_name.md) (creates conversion maps based on column names)
  - [execute_attr_map_tuple](../e/execute_attr_map_tuple.md) (performs actual tuple conversion using the map)
  - [ExecFindPartition](../E/ExecFindPartition.md) (partition routing)
  - [AfterTriggerExecute](../A/AfterTriggerExecute.md) (trigger execution)
  - [CopyFrom](../C/CopyFrom.md) (COPY command processing)

## Notes and Other Information
- The conversion map is allocated in the caller's memory context and both input tuple descriptors must remain valid for the lifetime of the map
- If no conversion is needed (physically compatible tuple descriptors), the setup functions return NULL instead of creating a map
- The workspace arrays are preallocated with size natts+1, where the extra entry (index 0) is reserved for NULL values
- This structure is central to PostgreSQL's ability to handle schema evolution, inheritance, and partitioning efficiently
- The conversion process is optimized to avoid repeated memory allocation by reusing the workspace arrays for multiple tuple conversions
- Used extensively in executor nodes, trigger systems, and logical replication where tuple format conversion is frequently required