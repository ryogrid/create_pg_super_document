# PlannerInfo

## Location
[src/include/optimizer/optimizer.h:34-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/optimizer.h#L34-L34)

## Overview
PlannerInfo is the central data structure used during query planning in PostgreSQL. It contains all the state information needed by the planner to analyze and optimize a single query level.

## Definition
```c
struct PlannerInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag        type;
    Query         *parse;                /* the Query being planned */
    PlannerGlobal *glob;                 /* global info for current planner run */
    Index          query_level;          /* 1 at the outermost Query */
    PlannerInfo   *parent_root;          /* NULL at outermost Query */
    
    /* Parameter management */
    List          *plan_params;          /* list of PlannerParamItems */
    Bitmapset     *outer_params;         /* paramIds of PARAM_EXEC Params */
    
    /* Relation arrays and information */
    struct RelOptInfo **simple_rel_array; /* base and other rels */
    int            simple_rel_array_size;
    RangeTblEntry **simple_rte_array;    /* associated rangetable entries */
    struct AppendRelInfo **append_rel_array;
    
    /* Relation sets */
    Relids         all_baserels;         /* all base relids in query */
    Relids         outer_join_rels;      /* all outer-join relids */
    Relids         all_query_rels;       /* all base + outer join relids */
    
    /* Join relation management */
    List          *join_rel_list;        /* all join-relation RelOptInfos */
    struct HTAB   *join_rel_hash;        /* hash table for lookups */
    List         **join_rel_level;       /* join relations by level */
    int            join_cur_level;       /* current level index */
    
    /* Query planning lists */
    List          *init_plans;           /* init SubPlans for query */
    List          *cte_plan_ids;         /* per-CTE-item subplan IDs */
    List          *multiexpr_params;     /* MULTIEXPR subquery outputs */
    List          *join_domains;         /* JoinDomains in query */
    List          *eq_classes;           /* active EquivalenceClasses */
    bool           ec_merging_done;      /* true once ECs are canonical */
    List          *canon_pathkeys;       /* canonical PathKeys */
    
    /* Join clause information */
    List          *left_join_clauses;    /* mergejoinable left outer joins */
    List          *right_join_clauses;   /* mergejoinable right outer joins */
    List          *full_join_clauses;    /* mergejoinable full joins */
    List          *join_info_list;       /* list of SpecialJoinInfos */
    int            last_rinfo_serial;    /* RestrictInfo serial counter */
    
    /* Result relation information */
    Relids         all_result_relids;    /* all result relids */
    Relids         leaf_result_relids;   /* leaf result relids only */
    List          *append_rel_list;      /* AppendRelInfos */
    List          *row_identity_vars;    /* RowIdentityVarInfos */
    List          *rowMarks;             /* PlanRowMarks */
    
    /* Placeholder management */
    List          *placeholder_list;     /* PlaceHolderInfos */
    struct PlaceHolderInfo **placeholder_array;
    int            placeholder_array_size;
    
    /* Foreign key information */
    List          *fkey_list;            /* ForeignKeyOptInfos */
    
    /* Pathkey information */
    List          *query_pathkeys;       /* desired pathkeys */
    List          *group_pathkeys;       /* groupClause pathkeys */
    int            num_groupby_pathkeys; /* GROUP BY pathkey count */
    List          *window_pathkeys;      /* window pathkeys */
    List          *distinct_pathkeys;    /* distinctClause pathkeys */
    List          *sort_pathkeys;        /* sortClause pathkeys */
    List          *setop_pathkeys;       /* set operator pathkeys */
    
    /* Partitioning and upper relations */
    List          *part_schemes;         /* partition schemes */
    List          *initial_rels;         /* rels being joined */
    List          *upper_rels[UPPERREL_FINAL + 1];  /* upper-rel RelOptInfos */
    struct PathTarget *upper_targets[UPPERREL_FINAL + 1]; /* upper-stage tlists */
    
    /* Processed clauses and target lists */
    List          *processed_groupClause;    /* processed GROUP BY */
    List          *processed_distinctClause; /* processed DISTINCT */
    List          *processed_tlist;          /* processed target list */
    List          *update_colnos;            /* UPDATE target columns */
    
    /* Planning workspace */
    AttrNumber    *grouping_map;         /* GroupingFunc fixup */
    List          *minmax_aggs;          /* MinMaxAggInfos */
    MemoryContext  planner_cxt;          /* context holding PlannerInfo */
    
    /* Cost estimation */
    Cardinality    total_table_pages;    /* pages in all tables */
    Selectivity    tuple_fraction;       /* tuple_fraction for query_planner */
    Cardinality    limit_tuples;         /* limit_tuples for query_planner */
    Index          qual_security_level;  /* minimum security level */
    
    /* Query characteristics flags */
    bool           hasJoinRTEs;          /* has JOIN RTEs */
    bool           hasLateralRTEs;       /* has LATERAL RTEs */
    bool           hasHavingQual;        /* had non-null HAVING */
    bool           hasPseudoConstantQuals; /* has pseudoconstant quals */
    bool           hasAlternativeSubPlans; /* has alternative subplans */
    bool           placeholdersFrozen;   /* no more PlaceHolderInfos */
    bool           hasRecursion;         /* planning recursive WITH */
    
    /* Aggregate information */
    List          *agginfos;             /* AggInfo structs */
    List          *aggtransinfos;        /* AggTransInfo structs */
    int            numOrderedAggs;       /* aggs with DISTINCT/ORDER BY */
    bool           hasNonPartialAggs;    /* any non-partial aggs */
    bool           hasNonSerialAggs;     /* any non-serializable partial aggs */
    
    /* Recursive query workspace */
    int            wt_param_id;          /* work table PARAM_EXEC ID */
    struct Path   *non_recursive_path;   /* non-recursive term path */
    
    /* createplan.c workspace */
    Relids         curOuterRels;         /* outer rels above current node */
    List          *curOuterParams;       /* not-yet-assigned NestLoopParams */
    
    /* setrefs.c workspace */
    bool          *isAltSubplan;         /* alternative subplan array */
    bool          *isUsedSubplan;        /* used subplan array */
    
    /* Extension hook data */
    void          *join_search_private;  /* private data for join_search_hook */
    
    /* Partition key modification flag */
    bool           partColsUpdated;      /* partition key columns modified */
};
```

## Detailed Description
PlannerInfo serves as the primary workspace for PostgreSQL's query planner. It maintains comprehensive state information throughout the planning process, including relation metadata, join information, pathkey specifications, and optimization constraints. The structure is designed to support multi-level query planning with nested subqueries, where each query level has its own PlannerInfo instance linked through the parent_root field.

The structure supports both bottom-up join planning (through join_rel_level arrays) and heuristic methods like GEQO. It tracks equivalence classes for join optimization, manages placeholder variables for complex expressions, and maintains comprehensive pathkey information for different SQL clauses (GROUP BY, ORDER BY, etc.).

## Parameters / Member Variables
### Core Planning State
- `type`: NodeTag for type identification
- `parse`: The Query node being planned
- `glob`: Global planner state shared across query levels  
- `query_level`: Nesting depth (1 for outermost query)
- `parent_root`: Parent PlannerInfo for subqueries

### Parameter Management
- `plan_params`: Expressions this query level provides to lower levels
- `outer_params`: PARAM_EXEC parameter IDs from outer query levels

### Relation Management
- `simple_rel_array`: Array of RelOptInfo pointers indexed by RTE index
- `simple_rel_array_size`: Allocated size of simple_rel_array
- `simple_rte_array`: Corresponding RangeTblEntry pointers for faster access
- `append_rel_array`: AppendRelInfo entries for appendrel children

### Relation Sets
- `all_baserels`: Relids set of all base relations
- `outer_join_rels`: Relids set of all outer join relations  
- `all_query_rels`: Combined base and outer join relations

### Join Planning
- `join_rel_list`: List of all considered join relations
- `join_rel_hash`: Hash table for efficient join relation lookup
- `join_rel_level`: Arrays of join relations by level for DP algorithm
- `join_cur_level`: Current level in dynamic programming search

### Query Components
- `init_plans`: Initialization subplans for the query
- `cte_plan_ids`: Subplan IDs for Common Table Expressions
- `multiexpr_params`: Parameters for MULTIEXPR subquery outputs
- `join_domains`: Join domain information for optimization

### Equivalence and Pathkeys
- `eq_classes`: Active equivalence classes for join optimization
- `ec_merging_done`: Flag indicating equivalence class canonicalization completion
- `canon_pathkeys`: Canonical pathkey representations
- `query_pathkeys`, `group_pathkeys`, etc.: Pathkeys for various SQL clauses

### Join Clause Analysis
- `left_join_clauses`: Mergejoinable outer join clauses with left nonnullable vars
- `right_join_clauses`: Mergejoinable outer join clauses with right nonnullable vars  
- `full_join_clauses`: Mergejoinable full join clauses
- `join_info_list`: List of SpecialJoinInfo structures

### Result Relations
- `all_result_relids`: All relations that are targets of modification
- `leaf_result_relids`: Only actual result tables (not partitioned parents)
- `append_rel_list`: Information about inheritance/partitioning relationships

### Cost Estimation
- `total_table_pages`: Total pages across all non-dummy tables
- `tuple_fraction`: Fraction of tuples expected to be retrieved
- `limit_tuples`: Upper bound on tuples from LIMIT clause
- `qual_security_level`: Minimum security level for query qualifiers

### Query Characteristics
- `hasJoinRTEs`: True if query contains explicit JOIN syntax
- `hasLateralRTEs`: True if query contains LATERAL references
- `hasHavingQual`: True if HAVING clause was present
- `hasPseudoConstantQuals`: True if any quals are pseudoconstant
- `hasRecursion`: True when planning recursive WITH queries

### Aggregate Processing
- `agginfos`: Information about aggregate functions
- `aggtransinfos`: Aggregate transition function information
- `numOrderedAggs`: Count of aggregates with DISTINCT/ORDER BY
- `hasNonPartialAggs`: True if any aggregates don't support partial aggregation

## Dependencies
- Functions called/Symbols referenced:
  - [Query](../Q/Query.md) (parse tree node)
  - [PlannerGlobal](PlannerGlobal.md) (global planning state)
  - [RelOptInfo](../R/RelOptInfo.md) (relation optimization info)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (join constraint info)
  - [EquivalenceClass](../E/EquivalenceClass.md) (equivalence class info)
  - [PathKey](PathKey.md) (sort ordering info)
  - [PlaceHolderInfo](PlaceHolderInfo.md) (placeholder variable info)

- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)() (main entry point)
  - [query_planner](../q/query_planner.md)() (core planning function)
  - [create_plan](../c/create_plan.md)() (plan tree creation)

## Notes and Other Information
PlannerInfo is the central hub of PostgreSQL's cost-based optimizer. It maintains state across all phases of planning from initial query analysis through final plan generation. The structure is carefully designed to support both standard dynamic programming join algorithms and alternative methods like genetic algorithm optimization (GEQO).

Key design principles include separation of global vs. per-query-level state, efficient lookup structures for large join problems, and comprehensive tracking of optimization opportunities. The structure grows and evolves during planning as more information becomes available about join costs, equivalence relationships, and optimization possibilities.

Memory management is handled through the planner_cxt memory context, ensuring all planning data structures are properly cleaned up after planning completes.