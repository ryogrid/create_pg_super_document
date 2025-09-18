# IndexOptInfo

## Location
src/include/optimizer/optimizer.h: 40 - 40

## Overview
IndexOptInfo represents all the information needed by the PostgreSQL query planner about a single index on a table. It contains metadata about the index structure, statistics, access method capabilities, and derived optimization information.

## Definition
```c
struct IndexOptInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag        type;

    /* Basic index identification */
    Oid            indexoid;         /* OID of the index relation */
    Oid            reltablespace;    /* tablespace of index (not table) */
    RelOptInfo    *rel;             /* back-link to index's table */

    /* Index size statistics (from pg_class and elsewhere) */
    BlockNumber    pages;           /* number of disk pages in index */
    Cardinality    tuples;          /* number of index tuples in index */
    int            tree_height;     /* index tree height, or -1 if unknown */

    /* Index structure information */
    int            ncolumns;        /* number of columns in index */
    int            nkeycolumns;     /* number of key columns in index */

    /* Column metadata arrays */
    int           *indexkeys;       /* table column numbers (0 for expressions) */
    Oid           *indexcollations; /* OIDs of collations of index columns */
    Oid           *opfamily;        /* OIDs of operator families for columns */
    Oid           *opcintype;       /* OIDs of opclass declared input data types */
    Oid           *sortopfamily;    /* OIDs of btree opfamilies, if orderable */
    bool          *reverse_sort;    /* is sort order descending? */
    bool          *nulls_first;     /* do NULLs come first in sort order? */
    bytea        **opclassoptions;  /* opclass-specific options for columns */
    bool          *canreturn;       /* which cols can be returned in index-only scan? */
    Oid            relam;           /* OID of the access method (in pg_am) */

    /* Index expressions and predicates */
    List          *indexprs;        /* expressions for non-simple index columns */
    List          *indpred;         /* predicate if partial index, else NIL */
    List          *indextlist;      /* targetlist representing index columns */
    List          *indrestrictinfo; /* parent rel's baserestrictinfo list */

    /* Index properties */
    bool           predOK;          /* true if index predicate matches query */
    bool           unique;          /* true if a unique index */
    bool           immediate;       /* is uniqueness enforced immediately? */
    bool           hypothetical;    /* true if index doesn't really exist */

    /* Access method capabilities (copied from IndexAmRoutine) */
    bool           amcanorderbyop;  /* does AM support order by operator? */
    bool           amoptionalkey;   /* can query omit key for some tuples? */
    bool           amsearcharray;   /* can AM search for any array element? */
    bool           amsearchnulls;   /* can AM search for NULL values? */
    bool           amhasgettuple;   /* does AM have amgettuple interface? */
    bool           amhasgetbitmap;  /* does AM have amgetbitmap interface? */
    bool           amcanparallel;   /* does AM support parallel scan? */
    bool           amcanmarkpos;    /* does AM have ammarkpos interface? */

    /* Cost estimation function pointer */
    void         (*amcostestimate)(struct PlannerInfo *, struct IndexPath *, 
                                  double, Cost *, Cost *, Selectivity *, 
                                  double *, double *);
};
```

## Detailed Description
IndexOptInfo is the planner's complete representation of an index that can be used for query optimization. It consolidates information from the system catalogs (pg_class, pg_index, pg_attribute, pg_opclass, etc.) with runtime-computed optimization data.

The structure supports both simple indexes on table columns and complex indexes with expressions or partial predicates. It includes detailed information about sort ordering, null handling, and access method capabilities that are essential for cost-based optimization decisions.

Key optimization features include tracking which index columns can support index-only scans (canreturn array), maintaining processed restriction clauses (indrestrictinfo), and providing access method-specific cost estimation through function pointers.

## Parameters / Member Variables
### Basic Identification
- `type`: NodeTag for type identification
- `indexoid`: System catalog OID of the index relation
- `reltablespace`: Tablespace containing the index files
- `rel`: Back-pointer to the RelOptInfo for the indexed table

### Size and Statistics
- `pages`: Number of disk pages occupied by the index
- `tuples`: Total number of index entries (tuples)
- `tree_height`: Height of index tree structure (-1 if unknown)

### Structure Information  
- `ncolumns`: Total number of columns in index (key + included)
- `nkeycolumns`: Number of key columns (excludes included columns)

### Column Metadata Arrays
- `indexkeys`: Array mapping index positions to table column numbers (0 for expression columns)
- `indexcollations`: Collation OIDs for each key column
- `opfamily`: Operator family OIDs for each key column
- `opcintype`: Input data types declared by operator classes
- `sortopfamily`: B-tree operator families for ordering (NULL for non-orderable indexes)
- `reverse_sort`: Whether each key column sorts in descending order
- `nulls_first`: Whether NULL values sort before non-NULL values
- `opclassoptions`: Operator class-specific configuration options
- `canreturn`: Which columns can be returned by index-only scans
- `relam`: Access method OID from pg_am

### Expressions and Predicates
- `indexprs`: Parsed expressions for functional index columns
- `indpred`: WHERE clause for partial indexes (NIL for complete indexes)
- `indextlist`: Target list representing index column values
- `indrestrictinfo`: Base table restriction clauses minus those implied by index predicate

### Index Properties
- `predOK`: True when index predicate is satisfied by query WHERE clause
- `unique`: True for unique indexes
- `immediate`: True if uniqueness is checked immediately (vs. deferred)
- `hypothetical`: True for hypothetical indexes (used in index advisors)

### Access Method Capabilities
- `amcanorderbyop`: Supports ORDER BY operations directly
- `amoptionalkey`: Some tuples can be found without specifying all key values
- `amsearcharray`: Can search for any element of an array value
- `amsearchnulls`: Can search for NULL values
- `amhasgettuple`: Supports tuple-at-a-time retrieval interface
- `amhasgetbitmap`: Supports bitmap scan interface
- `amcanparallel`: Supports parallel index scans
- `amcanmarkpos`: Supports mark/restore position operations

### Cost Estimation
- `amcostestimate`: Function pointer to access method's cost estimation routine

## Dependencies
- Functions called/Symbols referenced:
  - RelOptInfo (parent table information)
  - [List](../L/List.md) (expression and predicate lists)
  - [RestrictInfo](../R/RestrictInfo.md) (restriction clause information)
  - [IndexAmRoutine](IndexAmRoutine.md) (access method interface)

- Called from (representative examples):
  - [get_relation_info](../g/get_relation_info.md)() (builds IndexOptInfo during relation setup)
  - [create_index_paths](../c/create_index_paths.md)() (uses IndexOptInfo to generate index access paths)
  - [cost_index](../c/cost_index.md)() (cost estimation using IndexOptInfo)
  - [match_clauses_to_index](../m/match_clauses_to_index.md)() (clause matching for index usage)

## Notes and Other Information
IndexOptInfo is created during the relation setup phase of planning and remains immutable throughout optimization. The structure is designed to efficiently answer common optimization questions like "Can this index satisfy these WHERE clauses?" and "What is the cost of scanning this index?"

The arrays (indexkeys, indexcollations, etc.) are sized according to ncolumns or nkeycolumns and provide parallel information about each index column. The distinction between nkeycolumns and ncolumns is important for indexes with included (non-key) columns.

Access method capabilities are copied from the IndexAmRoutine to avoid repeated catalog lookups during optimization. The amcostestimate function pointer enables access method-specific cost calculations.

For partitioned indexes, some fields (sortopfamily, reverse_sort, nulls_first) may be NULL since individual partitions can have different access methods or properties.