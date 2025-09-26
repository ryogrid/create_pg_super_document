# RelOptInfo

## Location
src/include/nodes/pathnodes.h: 853 - 1046

## Overview
RelOptInfo is the central data structure in PostgreSQL query planning that contains per-relation information for optimization, including cost estimates, paths, constraints, and metadata for base relations, join relations, and other relation types.

## Definition
```c
typedef struct RelOptInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;
    RelOptKind  reloptkind;
    
    /* All relations included in this RelOptInfo */
    Relids      relids;
    
    /* Size estimates */
    Cardinality rows;
    
    /* Planner control flags */
    bool        consider_startup;
    bool        consider_param_startup;
    bool        consider_parallel;
    
    /* Default result targetlist and path information */
    struct PathTarget *reltarget;
    List       *pathlist;           /* Path structures */
    List       *ppilist;            /* ParamPathInfos used in pathlist */
    List       *partial_pathlist;   /* partial Paths */
    struct Path *cheapest_startup_path;
    struct Path *cheapest_total_path;
    struct Path *cheapest_unique_path;
    List       *cheapest_parameterized_paths;
    
    /* Parameterization information */
    Relids      direct_lateral_relids;
    Relids      lateral_relids;
    
    /* Base relation information */
    Index       relid;
    Oid         reltablespace;
    RTEKind     rtekind;
    AttrNumber  min_attr;
    AttrNumber  max_attr;
    Relids     *attr_needed;
    int32      *attr_widths;
    
    /* Additional base relation fields */
    Bitmapset  *notnullattnums;
    Relids      nulling_relids;
    List       *lateral_vars;
    Relids      lateral_referencers;
    List       *indexlist;          /* IndexOptInfo structures */
    List       *statlist;           /* StatisticExtInfo structures */
    BlockNumber pages;
    Cardinality tuples;
    double      allvisfrac;
    Bitmapset  *eclass_indexes;
    PlannerInfo *subroot;           /* if subquery */
    List       *subplan_params;     /* if subquery */
    int         rel_parallel_workers;
    uint32      amflags;
    
    /* Foreign table/join information */
    Oid         serverid;
    Oid         userid;
    bool        useridiscurrent;
    struct FdwRoutine *fdwroutine;
    void       *fdw_private;
    
    /* Uniqueness cache */
    List       *unique_for_rels;
    List       *non_unique_for_rels;
    
    /* Restriction and join information */
    List       *baserestrictinfo;
    QualCost    baserestrictcost;
    Index       baserestrict_min_security;
    List       *joininfo;
    bool        has_eclass_joins;
    
    /* Partitionwise join support */
    bool        consider_partitionwise_join;
    
    /* Inheritance information */
    struct RelOptInfo *parent;
    struct RelOptInfo *top_parent;
    Relids      top_parent_relids;
    
    /* Partitioning information */
    PartitionScheme part_scheme;
    int         nparts;
    struct PartitionBoundInfoData *boundinfo;
    bool        partbounds_merged;
    List       *partition_qual;
    struct RelOptInfo **part_rels;
    Bitmapset  *live_parts;
    Relids      all_partrels;
    List      **partexprs;
    List      **nullable_partexprs;
} RelOptInfo;
```

## Detailed Description
RelOptInfo is the cornerstone data structure for PostgreSQL query optimization, representing all types of relations that can appear in query planning. It serves multiple purposes:

1. **Base Relations**: Represents individual tables, subqueries, or functions in the FROM clause
2. **Join Relations**: Represents the result of joining two or more base relations
3. **Other Relations**: Represents special relation types like append relation members
4. **Upper Relations**: Represents post-scan/join processing steps like aggregation

The structure contains comprehensive information needed for cost-based optimization, including size estimates, available access paths, constraints, indexes, and partitioning information. It supports advanced PostgreSQL features like parallel execution, foreign data wrappers, partitioning, and lateral references.

## Parameters / Member Variables
- `type`: NodeTag identifier for the structure
- `reloptkind`: Type of relation (base, join, other, upper)
- `relids`: Set of relation identifiers included in this RelOptInfo
- `rows`: Estimated number of result tuples after applying restrictions
- `consider_startup`: Whether to keep paths with cheap startup costs
- `consider_param_startup`: Same as above for parameterized paths
- `consider_parallel`: Whether to consider parallel execution paths
- `reltarget`: Default output targetlist for paths scanning this relation
- `pathlist`: List of available Path nodes for accessing this relation
- `cheapest_startup_path`: Path with lowest startup cost among unparameterized paths
- `cheapest_total_path`: Path with lowest total cost
- `relid`: Range table index for base relations
- `rtekind`: Kind of range table entry (table, subquery, function, etc.)
- `indexlist`: Available indexes for table access (IndexOptInfo list)
- `pages`/`tuples`: Physical size estimates from pg_class
- `part_scheme`: Partitioning scheme for partitioned relations
- `baserestrictinfo`: Non-join qualification clauses for base relations
- `joininfo`: Join clauses involving this relation

## Dependencies
- Functions called/Symbols referenced:
  - RelOptKind (line 859)
  - Cardinality (lines 871, 943)
  - PathTarget (line 887)
  - RTEKind (line 916)
  - PartitionScheme (line 1009)
  - PartitionBoundInfoData (line 1017)
  - QualCost (line 981)
  - FdwRoutine (line 964)
- Called from (representative examples):
  - Used extensively throughout the PostgreSQL optimizer
  - Core structure in src/backend/optimizer/ modules
  - Referenced in path generation, join planning, and cost estimation

## Notes and Other Information
- Central to PostgreSQL cost-based query optimization
- Supports complex features like partitioning, parallel execution, and foreign data access
- Memory management handled by planner memory contexts
- Contains both computed information (like cheapest paths) and cached data (like uniqueness proofs)
- Different fields are relevant depending on the relation type (base vs. join vs. other)
- Critical for partition-wise joins and partition pruning optimizations
- Located in pathnodes.h:853-1046 as part of the planner data structures