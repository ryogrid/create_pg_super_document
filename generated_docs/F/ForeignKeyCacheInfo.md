# ForeignKeyCacheInfo

## Location
[src/include/utils/rel.h:273-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rel.h#L273-L296)

## Overview
ForeignKeyCacheInfo is a structure that caches foreign key constraint information in the relation cache, providing efficient access to constraint metadata without repeated catalog lookups.

## Definition

```c
typedef struct ForeignKeyCacheInfo
{
	pg_node_attr(no_equal, no_read, no_query_jumble)

	NodeTag		type;
	/* oid of the constraint itself */
	Oid			conoid;
	/* relation constrained by the foreign key */
	Oid			conrelid;
	/* relation referenced by the foreign key */
	Oid			confrelid;
	/* number of columns in the foreign key */
	int			nkeys;

	/*
	 * these arrays each have nkeys valid entries:
	 */
	/* cols in referencing table */
	AttrNumber	conkey[INDEX_MAX_KEYS] pg_node_attr(array_size(nkeys));
	/* cols in referenced table */
	AttrNumber	confkey[INDEX_MAX_KEYS] pg_node_attr(array_size(nkeys));
	/* PK = FK operator OIDs */
	Oid			conpfeqop[INDEX_MAX_KEYS] pg_node_attr(array_size(nkeys));
} ForeignKeyCacheInfo;
```
## Detailed Description
ForeignKeyCacheInfo serves as a cached representation of foreign key constraint information derived from the pg_constraint system catalog. This structure is designed to provide efficient access to foreign key metadata that is frequently needed by the query planner and other system components, without requiring repeated expensive catalog lookups.

The structure is implemented as a Node subclass, making it compatible with PostgreSQL's standard node copying and manipulation functions. However, it's designed as a "flat" structure without substructure, which allows efficient memory management using simple list operations.

The structure caches the essential information about a foreign key constraint, including the OIDs of both the referencing and referenced relations, the column mappings between them, and the equality operators used for constraint checking. This information is particularly valuable to the query planner for optimization decisions involving foreign key relationships.

## Parameters / Member Variables
- `type`: NodeTag identifier for the node type system
- `conoid`: Object identifier of the foreign key constraint itself in pg_constraint
- `conrelid`: OID of the relation that contains the foreign key (referencing table)
- `confrelid`: OID of the relation that is referenced by the foreign key (referenced table)
- `nkeys`: Number of columns participating in the foreign key constraint
- `conkey[INDEX_MAX_KEYS] pg_node_attr(array_size(nkeys))`: Array of attribute numbers for columns in the referencing table (size limited by INDEX_MAX_KEYS)
- `confkey[INDEX_MAX_KEYS] pg_node_attr(array_size(nkeys))`: Array of attribute numbers for columns in the referenced table (size limited by INDEX_MAX_KEYS)
- `conpfeqop[INDEX_MAX_KEYS] pg_node_attr(array_size(nkeys))`: Array of OIDs for the equality operators used to compare primary key and foreign key values (size limited by INDEX_MAX_KEYS)
## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (constant defining maximum array size)
  - NodeTag, Oid, AttrNumber (basic PostgreSQL types)
- Called from (representative examples):
  - [RelationGetFKeyList](../R/RelationGetFKeyList.md) (relcache management)
  - [get_relation_foreign_keys](../g/get_relation_foreign_keys.md) (planner interface)
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md) (DDL operations)
  - [CloneFkReferencing](../C/CloneFkReferencing.md) (partition management)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md) (partition operations)

## Notes and Other Information
- The structure is primarily used by the query planner to make optimization decisions based on foreign key relationships
- Arrays are fixed-size with INDEX_MAX_KEYS elements, but only the first nkeys entries contain valid data
- The "flat" design without substructure allows efficient memory management and list operations
- [Node](../N/Node.md) attributes (no_equal, no_read, no_query_jumble) indicate this structure should not participate in certain node operations
- This caching mechanism significantly improves performance for queries involving tables with many foreign key relationships
- The structure has grown over time as new use cases have been identified, originally focused on planner needs but now including constraint OIDs for other purposes