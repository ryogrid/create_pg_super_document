# IndexInfo

## Location
src/include/nodes/execnodes.h: 183 - 211

## Overview
IndexInfo is a comprehensive data structure that holds all necessary information for constructing and managing index entries in PostgreSQL, used in both bulk index builds and individual tuple insertions.

## Definition


## Detailed Description
IndexInfo serves as the central metadata structure for index operations in PostgreSQL. It contains both structural information about the index (column mappings, expressions, predicates) and operational state (uniqueness constraints, build status, concurrency flags). The structure supports complex index types including expression indexes, partial indexes, exclusion constraints, and unique constraints. It maintains both the parse tree representations and compiled execution states of expressions and predicates for efficient evaluation during index operations.

## Parameters / Member Variables
- : Standard PostgreSQL node tag for type identification
- : Total number of columns in the index including both key and included columns
- : Number of key columns in the index (excluding included columns)
- : Array mapping index columns to underlying relation attribute numbers (zero indicates expression columns)
- : List of expression trees for computed index columns, NIL if none
- : Compiled execution states for expressions, NIL if none
- : Expression tree for partial index predicate, NIL for complete indexes
- : Compiled execution state for predicate evaluation
- : Array of exclusion constraint operators, one per column, NULL if none
- : Array of underlying function OIDs for exclusion operators
- : Array of operator class strategy numbers for exclusion operators
- : Array of operators for unique constraint checking, similar to exclusion operators
- : Array of underlying function OIDs for unique constraint operators
- : Array of strategy numbers for unique constraint operators
- : Boolean flag indicating whether this is a unique index
- : Flag for NULLS NOT DISTINCT unique constraint behavior
- : Flag indicating whether the index is ready to accept new tuples
- : Flag indicating whether IndexUnchanged status has been determined
- : Cached hint for retail inserts, indicates if index columns are unchanged
- : Flag indicating concurrent index build operation
- : Flag set if broken HOT chains are detected during build
- : Flag indicating whether this is a summarizing index (e.g., BRIN)
- : Number of parallel workers requested for index build (excluding leader)
- : OID of the index access method
- : Private cache area for access method-specific data
- : Memory context that holds this IndexInfo structure

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (maximum number of index key columns)
  - ExprState (compiled expression evaluation states)
  - List (PostgreSQL list structure for expressions and predicates)
- Called from (representative examples):
  - index_build (bulk index construction)
  - index_insert (individual tuple insertion)
  - BrinInsert, GinInsert, GistInsert (access method specific insertion)
  - DefineIndex (index creation command processing)

## Notes and Other Information
- Fields ii_Concurrent, ii_BrokenHotChain, and ii_ParallelWorkers are used only during index build and are conventionally zeroed otherwise
- The structure supports all PostgreSQL index types including B-tree, Hash, GiST, GIN, SP-GiST, and BRIN
- Expression and predicate evaluation states are maintained for performance optimization during index operations
- The IndexUnchanged hint mechanism helps optimize retail inserts by avoiding unnecessary index updates
- Memory management is handled through the ii_Context memory context for proper cleanup