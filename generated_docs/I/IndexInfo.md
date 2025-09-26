# IndexInfo

## Location
[src/include/nodes/execnodes.h:183-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L183-L211)

## Overview
IndexInfo is a comprehensive data structure that holds all necessary information for constructing and managing index entries in PostgreSQL, used in both bulk index builds and individual tuple insertions.

## Definition

```c
typedef struct IndexInfo
{
	NodeTag		type;
	int			ii_NumIndexAttrs;	/* total number of columns in index */
	int			ii_NumIndexKeyAttrs;	/* number of key columns in index */
	AttrNumber	ii_IndexAttrNumbers[INDEX_MAX_KEYS];
	List	   *ii_Expressions; /* list of Expr */
	List	   *ii_ExpressionsState;	/* list of ExprState */
	List	   *ii_Predicate;	/* list of Expr */
	ExprState  *ii_PredicateState;
	Oid		   *ii_ExclusionOps;	/* array with one entry per column */
	Oid		   *ii_ExclusionProcs;	/* array with one entry per column */
	uint16	   *ii_ExclusionStrats; /* array with one entry per column */
	Oid		   *ii_UniqueOps;	/* array with one entry per column */
	Oid		   *ii_UniqueProcs; /* array with one entry per column */
	uint16	   *ii_UniqueStrats;	/* array with one entry per column */
	bool		ii_Unique;
	bool		ii_NullsNotDistinct;
	bool		ii_ReadyForInserts;
	bool		ii_CheckedUnchanged;
	bool		ii_IndexUnchanged;
	bool		ii_Concurrent;
	bool		ii_BrokenHotChain;
	bool		ii_Summarizing;
	int			ii_ParallelWorkers;
	Oid			ii_Am;
	void	   *ii_AmCache;
	MemoryContext ii_Context;
} IndexInfo;
```
## Detailed Description
IndexInfo serves as the central metadata structure for index operations in PostgreSQL. It contains both structural information about the index (column mappings, expressions, predicates) and operational state (uniqueness constraints, build status, concurrency flags). The structure supports complex index types including expression indexes, partial indexes, exclusion constraints, and unique constraints. It maintains both the parse tree representations and compiled execution states of expressions and predicates for efficient evaluation during index operations.

## Parameters / Member Variables
- `type`: Standard PostgreSQL node tag for type identification
- `ii_NumIndexAttrs`: Total number of columns in the index including both key and included columns
- `ii_NumIndexKeyAttrs`: Number of key columns in the index (excluding included columns)
- `ii_IndexAttrNumbers[INDEX_MAX_KEYS]`: Array mapping index columns to underlying relation attribute numbers (zero indicates expression columns)
- `*ii_Expressions`: List of expression trees for computed index columns, NIL if none
- `*ii_ExpressionsState`: Compiled execution states for expressions, NIL if none
- `*ii_Predicate`: Expression tree for partial index predicate, NIL for complete indexes
- `*ii_PredicateState`: Compiled execution state for predicate evaluation
- `*ii_ExclusionOps`: Array of exclusion constraint operators, one per column, NULL if none
- `*ii_ExclusionProcs`: Array of underlying function OIDs for exclusion operators
- `*ii_ExclusionStrats`: Array of operator class strategy numbers for exclusion operators
- `*ii_UniqueOps`: Array of operators for unique constraint checking, similar to exclusion operators
- `*ii_UniqueProcs`: Array of underlying function OIDs for unique constraint operators
- `*ii_UniqueStrats`: Array of strategy numbers for unique constraint operators
- `ii_Unique`: Boolean flag indicating whether this is a unique index
- `ii_NullsNotDistinct`: Flag for NULLS NOT DISTINCT unique constraint behavior
- `ii_ReadyForInserts`: Flag indicating whether the index is ready to accept new tuples
- `ii_CheckedUnchanged`: Flag indicating whether IndexUnchanged status has been determined
- `ii_IndexUnchanged`: Cached hint for retail inserts, indicates if index columns are unchanged
- `ii_Concurrent`: Flag indicating concurrent index build operation
- `ii_BrokenHotChain`: Flag set if broken HOT chains are detected during build
- `ii_Summarizing`: Flag indicating whether this is a summarizing index (e.g., BRIN)
- `ii_ParallelWorkers`: Number of parallel workers requested for index build (excluding leader)
- `ii_Am`: OID of the index access method
- `*ii_AmCache`: Private cache area for access method-specific data
- `ii_Context`: Memory context that holds this IndexInfo structure
## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (maximum number of index key columns)
  - [ExprState](../E/ExprState.md) (compiled expression evaluation states)
  - [List](../L/List.md) (PostgreSQL list structure for expressions and predicates)
- Called from (representative examples):
  - [index_build](../i/index_build.md) (bulk index construction)
  - [index_insert](../i/index_insert.md) (individual tuple insertion)
  - BrinInsert, GinInsert, GistInsert (access method specific insertion)
  - [DefineIndex](../D/DefineIndex.md) (index creation command processing)

## Notes and Other Information
- Fields ii_Concurrent, ii_BrokenHotChain, and ii_ParallelWorkers are used only during index build and are conventionally zeroed otherwise
- The structure supports all PostgreSQL index types including B-tree, Hash, GiST, GIN, SP-GiST, and BRIN
- Expression and predicate evaluation states are maintained for performance optimization during index operations
- The IndexUnchanged hint mechanism helps optimize retail inserts by avoiding unnecessary index updates
- Memory management is handled through the ii_Context memory context for proper cleanup