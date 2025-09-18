# RestrictInfo

## Location
src/include/nodes/pathnodes.h: 2559 - 2711

## Overview
RestrictInfo is a comprehensive data structure that represents restriction clauses (WHERE or JOIN/ON conditions) with extensive metadata used by the PostgreSQL optimizer for query planning, cost estimation, and join optimization.

## Definition
```c
typedef struct RestrictInfo
{
    NodeTag     type;
    
    /* the represented clause of WHERE or JOIN */
    Expr       *clause;
    
    /* true if clause was pushed down in level */
    bool        is_pushed_down;
    
    /* see comment above */
    bool        can_join pg_node_attr(equal_ignore);
    
    /* see comment above */
    bool        pseudoconstant pg_node_attr(equal_ignore);
    
    /* see comment above */
    bool        has_clone;
    bool        is_clone;
    
    /* true if known to contain no leaked Vars */
    bool        leakproof pg_node_attr(equal_ignore);
    
    /* indicates if clause contains any volatile functions */
    VolatileFunctionStatus has_volatile pg_node_attr(equal_ignore);
    
    /* see comment above */
    Index       security_level;
    
    /* number of base rels in clause_relids */
    int         num_base_rels pg_node_attr(equal_ignore);
    
    /* The relids (varnos+varnullingrels) actually referenced in the clause: */
    Relids      clause_relids pg_node_attr(equal_ignore);
    
    /* The set of relids required to evaluate the clause: */
    Relids      required_relids;
    
    /* Relids above which we cannot evaluate the clause */
    Relids      incompatible_relids;
    
    /* If an outer-join clause, the outer-side relations, else NULL: */
    Relids      outer_relids;
    
    /* Relids in the left/right side of the clause */
    Relids      left_relids pg_node_attr(equal_ignore);
    Relids      right_relids pg_node_attr(equal_ignore);
    
    /* Modified clause with RestrictInfos. NULL unless clause is an OR clause. */
    Expr       *orclause pg_node_attr(equal_ignore);
    
    /* Serial number of this RestrictInfo */
    int         rinfo_serial;
    
    /* Generating EquivalenceClass. NULL unless clause is potentially redundant. */
    EquivalenceClass *parent_ec pg_node_attr(copy_as_scalar, equal_ignore, read_write_ignore);
    
    /* cache space for cost and selectivity */
    QualCost    eval_cost pg_node_attr(equal_ignore);
    Selectivity norm_selec pg_node_attr(equal_ignore);
    Selectivity outer_selec pg_node_attr(equal_ignore);
    
    /* opfamilies containing clause operator; valid if clause is mergejoinable, else NIL */
    List       *mergeopfamilies pg_node_attr(equal_ignore);
    
    /* cache space for mergeclause processing; NULL if not yet set */
    EquivalenceClass *left_ec pg_node_attr(copy_as_scalar, equal_ignore, read_write_ignore);
    EquivalenceClass *right_ec pg_node_attr(copy_as_scalar, equal_ignore, read_write_ignore);
    EquivalenceMember *left_em pg_node_attr(copy_as_scalar, equal_ignore);
    EquivalenceMember *right_em pg_node_attr(copy_as_scalar, equal_ignore);
    
    /* List of MergeScanSelCache structs */
    List       *scansel_cache pg_node_attr(copy_as(NIL), equal_ignore, read_write_ignore);
    
    /* transient workspace for use while considering a specific join path */
    bool        outer_is_left pg_node_attr(equal_ignore);
    
    /* copy of clause operator; valid if clause is hashjoinable, else InvalidOid */
    Oid         hashjoinoperator pg_node_attr(equal_ignore);
    
    /* cache space for hashclause processing */
    Selectivity left_bucketsize pg_node_attr(equal_ignore);
    Selectivity right_bucketsize pg_node_attr(equal_ignore);
    Selectivity left_mcvfreq pg_node_attr(equal_ignore);
    Selectivity right_mcvfreq pg_node_attr(equal_ignore);
    
    /* hash equality operators used for memoize nodes, else InvalidOid */
    Oid         left_hasheqoperator pg_node_attr(equal_ignore);
    Oid         right_hasheqoperator pg_node_attr(equal_ignore);
} RestrictInfo;
```

## Detailed Description
RestrictInfo is a central data structure in PostgreSQL's query optimizer that wraps restriction clauses (WHERE conditions and JOIN/ON clauses) with extensive metadata needed for optimal query planning. Each RestrictInfo node represents one AND-ed component of a restriction condition and contains information about relation dependencies, join applicability, security levels, cost estimates, and caching data for various join algorithms. The structure supports complex scenarios including outer joins, security-barrier conditions, equivalence classes, and parallel processing constraints. RestrictInfos are distributed across different RelOptInfo structures based on which relations they reference, enabling efficient join tree construction and clause placement optimization.

## Parameters / Member Variables
- `clause`: The actual restriction expression (WHERE or JOIN/ON clause)
- `is_pushed_down`: Flag indicating if the clause was pushed down from a higher level in the join tree
- `can_join`: True if the clause can potentially be used for merge or hash joins
- `pseudoconstant`: True if the clause contains no current-level Vars and no volatile functions
- `has_clone/is_clone`: Flags for managing multiple versions of clauses created for outer join handling
- `leakproof`: True if the clause is known to contain no information-leaking functions
- `has_volatile`: Indicates presence of volatile functions in the clause
- `security_level`: Security level for barrier conditions (higher values = less trusted sources)
- `num_base_rels`: Count of base relations referenced in clause_relids
- `clause_relids`: Set of relation IDs actually referenced in the clause
- `required_relids`: Minimum set of relations needed to evaluate the clause
- `incompatible_relids`: Outer-join relation IDs above which clause cannot be evaluated
- `outer_relids`: For outer join clauses, the relations on the outer side
- `left_relids/right_relids`: Relations referenced in left and right sides of binary operators
- `orclause`: Modified version with RestrictInfos inserted (for OR clauses only)
- `rinfo_serial`: Unique identifier within PlannerInfo context
- `parent_ec`: Generating EquivalenceClass for redundant clauses
- `eval_cost`: Cached evaluation cost estimate
- `norm_selec/outer_selec`: Cached selectivity estimates for different join types
- `mergeopfamilies`: Operator families for merge joins
- `left_ec/right_ec`: EquivalenceClasses for merge clause processing
- `left_em/right_em`: EquivalenceMembers for left and right sides
- `scansel_cache`: Cache of MergeScanSelCache structures
- `outer_is_left`: Workspace flag for join path consideration
- `hashjoinoperator`: Operator OID for hash joins
- `left_bucketsize/right_bucketsize`: Hash join bucket size estimates
- `left_mcvfreq/right_mcvfreq`: Most common value frequencies for hash joins
- `left_hasheqoperator/right_hasheqoperator`: Hash equality operators for memoization

## Dependencies
- Functions called/Symbols referenced:
  - [Expr](../E/Expr.md) (clause expression)
  - VolatileFunctionStatus (volatility tracking)
  - EquivalenceClass (equivalence processing)
  - [EquivalenceMember](../E/EquivalenceMember.md) (equivalence members)
  - QualCost (cost estimation)
  - Relids (relation ID sets)
- Called from (representative examples):
  - [make_restrictinfo](../m/make_restrictinfo.md) (restrictinfo.c:47)
  - [build_implied_join_equality](../b/build_implied_join_equality.md) (initsplan.c:3086)
  - [make_restrictinfo_internal](../m/make_restrictinfo_internal.md) (various locations)

## Notes and Other Information
- RestrictInfos are created for each AND-ed component of restriction conditions, never for OR clauses at the top level
- The structure supports sophisticated outer join semantics with required_relids manipulation to prevent premature evaluation
- Serial numbers enable detection of redundant clauses, especially important for outer join identity transformations
- Security levels implement security-barrier functionality by controlling evaluation order of clauses from different trust levels  
- Extensive caching fields optimize repeated cost and selectivity calculations during planning
- Clone management handles multiple versions of clauses needed for complex outer join scenarios
- EquivalenceClass integration enables advanced join optimization through implied equality detection