# PartitionKeyData

## Location
[src/include/utils/partcache.h:25-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/partcache.h#L25-L48)

## Overview
PartitionKeyData is a struct that stores comprehensive information about the partition key of a relation, including partitioning strategy, attributes, operators, and type information necessary for PostgreSQL's table partitioning functionality.

## Definition

```c
typedef struct PartitionKeyData
{
	PartitionStrategy strategy; /* partitioning strategy */
	int16		partnatts;		/* number of columns in the partition key */
	AttrNumber *partattrs;		/* attribute numbers of columns in the
								 * partition key or 0 if it's an expr */
	List	   *partexprs;		/* list of expressions in the partitioning
								 * key, one for each zero-valued partattrs */

	Oid		   *partopfamily;	/* OIDs of operator families */
	Oid		   *partopcintype;	/* OIDs of opclass declared input data types */
	FmgrInfo   *partsupfunc;	/* lookup info for support funcs */

	/* Partitioning collation per attribute */
	Oid		   *partcollation;

	/* Type information per attribute */
	Oid		   *parttypid;
	int32	   *parttypmod;
	int16	   *parttyplen;
	bool	   *parttypbyval;
	char	   *parttypalign;
	Oid		   *parttypcoll;
}			PartitionKeyData;
```
## Detailed Description
PartitionKeyData is the core data structure that encapsulates all metadata required for table partitioning in PostgreSQL. It serves as a comprehensive descriptor that contains both the logical partitioning specification (strategy, attributes/expressions) and the operational metadata (operator families, type information, collations) needed to perform partition-related operations such as partition pruning, constraint checking, and routing tuples to appropriate partitions.

The structure supports both column-based partitioning (using attribute numbers) and expression-based partitioning (using arbitrary expressions), making it flexible enough to handle complex partitioning schemes. The type and operator information is essential for the query planner and executor to make informed decisions about partition operations.

## Parameters / Member Variables
- : The partitioning strategy (RANGE, LIST, HASH) that determines how partition bounds are interpreted and how tuples are routed
- : The number of columns/expressions in the partition key, determining the size of all the arrays that follow
- : Array of attribute numbers for columns used in partitioning; contains 0 for positions that use expressions instead of columns
- : List of expressions used for partitioning, corresponding to zero entries in partattrs array
- : Array of OIDs identifying the operator families used for comparing partition key values
- : Array of OIDs specifying the input data types declared by the operator classes
- : Array of function manager info structures for support functions needed by the partitioning operators
- : Array of OIDs specifying the collation to use for each partition key attribute
- : Array of OIDs identifying the data type of each partition key attribute
- : Array of type modifiers for each partition key attribute (e.g., precision for numeric types)
- : Array of storage lengths for each partition key attribute type
- : Array of boolean flags indicating whether each partition key attribute type is passed by value
- : Array of alignment requirements for each partition key attribute type
- : Array of OIDs specifying the default collation for each partition key attribute type

## Dependencies
- Functions called/Symbols referenced:
  - PartitionStrategy (enum defining partitioning strategies)
- Called from (representative examples):
  - RelationBuildPartitionKey (builds PartitionKeyData from catalog information)
- Type alias:
  - PartitionKey (pointer to PartitionKeyData defined in partdefs.h)

## Notes and Other Information
- This structure is typically allocated in a long-lived memory context as it represents cached metadata that persists for the lifetime of a relation's cache entry
- The arrays (partattrs, partopfamily, etc.) are all of size partnatts, providing parallel information for each partition key component
- The structure supports mixed column and expression partitioning within a single partition key
- Type information is duplicated from the system catalogs for performance, avoiding repeated catalog lookups during partition operations
- The FmgrInfo structures cache function call overhead, making partition key comparisons more efficient
- This structure is fundamental to PostgreSQL's declarative partitioning feature introduced in version 10