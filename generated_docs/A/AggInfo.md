# AggInfo

## Location
[src/include/nodes/pathnodes.h:3365-3390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3365-L3390)

## Overview
AggInfo is a structure representing aggregate function metadata in pg_dump, serving as a superset of FuncInfo to store information needed for dumping aggregate function definitions.

## Definition

```c
typedef struct AggInfo
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;

	/*
	 * List of Aggref exprs that this state value is for.
	 *
	 * There will always be at least one, but there can be multiple identical
	 * Aggref's sharing the same per-agg.
	 */
	List	   *aggrefs;

	/* Transition state number for this aggregate */
	int			transno;

	/*
	 * "shareable" is false if this agg cannot share state values with other
	 * aggregates because the final function is read-write.
	 */
	bool		shareable;

	/* Oid of the final function, or InvalidOid if none */
	Oid			finalfn_oid;
} AggInfo;
```
## Detailed Description
AggInfo is used by pg_dump to store metadata about aggregate functions during the database dump process. It extends FuncInfo by inheritance, meaning it contains all the fields of a FuncInfo structure plus potentially additional aggregate-specific fields. Currently, no additional fields beyond those in FuncInfo are required, but the structure is designed to allow for future expansion if aggregate-specific metadata needs to be stored. This structure is part of pg_dump's internal representation of database objects that need to be dumped and restored.

## Parameters / Member Variables
- `type`: Node tag for type identification
- `aggrefs`: List of Aggref expressions that this state value represents (there will always be at least one, but there can be multiple identical Aggrefs sharing the same per-agg)
- `transno`: Transition state number for this aggregate  
- `shareable`: Boolean flag indicating whether this aggregate can share state values with other aggregates (false if final function is read-write)
- `finalfn_oid`: OID of the final function, or InvalidOid if none

## Dependencies
- Functions called/Symbols referenced:
  - FuncInfo
- Called from (representative examples):
  - [getAggregates](../g/getAggregates.md)
  - [dumpDumpableObject](../d/dumpDumpableObject.md)
  - [format_aggregate_signature](../f/format_aggregate_signature.md)
  - [dumpAgg](../d/dumpAgg.md)
  - fmtQualifiedDumpable
  - [get_agg_clause_costs](../g/get_agg_clause_costs.md)
  - [find_compatible_agg](../f/find_compatible_agg.md)
  - [preprocess_aggref](../p/preprocess_aggref.md)

## Notes and Other Information
- This structure is specific to pg_dump and is not used in the main PostgreSQL backend
- The comment indicates that no additional fields beyond FuncInfo are currently needed, but the structure allows for future extension
- Part of pg_dump's object metadata system for tracking database objects during dump/restore operations
- The structure follows the pattern of other pg_dump Info structures that extend base structures for specific object types
- Used in both the dumping process (extracting aggregate definitions) and during dependency analysis
- Located in src/bin/pg_dump/pg_dump.h:245-249