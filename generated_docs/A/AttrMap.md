# AttrMap

## Location
[src/include/access/attmap.h:34-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/attmap.h#L34-L38)

## Overview
AttrMap is a structure that maps attribute numbers between a pair of relations (typically inheritance parent and child relations) whose common columns may have different attribute numbers.

## Definition

```c
typedef struct AttrMap
{
	AttrNumber *attnums;
	int			maplen;
} AttrMap;
```
## Detailed Description
The AttrMap structure provides a mapping mechanism for attribute numbers when dealing with relations that share common columns but have different attribute numbering schemes. This situation commonly arises in PostgreSQL when:

- Columns are ordered differently between two relations
- Relations have dropped columns at different positions 
- Working with inheritance hierarchies where parent and child tables may have evolved differently

The mapping works by storing an array of attribute numbers where each position in the array corresponds to an attribute in the 'output' relation, and the value at that position indicates the corresponding attribute number in the 'input' relation. When an attribute doesn't exist in the input relation or has been dropped, the corresponding array element is set to 0.

The structure is designed to handle the full attribute space of the output relation, including accounting for any dropped attributes by setting their corresponding mapping entries to 0.

## Parameters / Member Variables
- `*attnums`: Array of AttrNumber values that maps each attribute position in the output relation to the corresponding attribute number in the input relation. Elements are set to 0 for dropped or non-existent attributes.
- `maplen`: The number of attributes in the 'output' relation, including any dropped attributes. This determines the size of the attnums array.
## Dependencies
- Functions called/Symbols referenced:
  - AttrNumber (attribute number type)

- Called from (representative examples):
  - [make_attrmap](../m/make_attrmap.md) (creates and initializes AttrMap structures)
  - [free_attrmap](../f/free_attrmap.md) (deallocates AttrMap structures)
  - [build_attrmap_by_position](../b/build_attrmap_by_position.md) (builds mapping based on positional correspondence)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md) (builds mapping based on column name matching)
  - [execute_attr_map_tuple](../e/execute_attr_map_tuple.md) (applies attribute mapping to tuples)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md) (applies attribute mapping to tuple slots)
  - [ExecFindPartition](../E/ExecFindPartition.md) (used in partition routing)
  - [logicalrep_partition_open](../l/logicalrep_partition_open.md) (used in logical replication)

## Notes and Other Information
- [AttrMap](AttrMap.md) is primarily used in scenarios involving table inheritance, partitioning, and logical replication where column layouts may differ between related tables
- The structure is allocated using PostgreSQL's memory management functions (palloc/palloc0)
- Memory management functions like make_attrmap() and free_attrmap() are provided for proper allocation and deallocation
- The mapping is unidirectional from input to output relation
- Zero values in the attnums array indicate attributes that don't exist or have been dropped in the input relation
- This structure is essential for tuple conversion operations when data needs to be transformed between different table schemas