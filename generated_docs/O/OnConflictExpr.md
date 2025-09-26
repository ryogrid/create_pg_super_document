# OnConflictExpr

## Location
[src/include/nodes/primnodes.h:2321-2337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2321-L2337)

## Overview
OnConflictExpr represents an ON CONFLICT DO ... expression in PostgreSQL, handling conflict resolution for INSERT statements by specifying actions when unique constraint violations occur.

## Definition
```c
typedef struct OnConflictExpr
{
    NodeTag             type;
    OnConflictAction    action;         /* DO NOTHING or UPDATE? */

    /* Arbiter */
    List               *arbiterElems;   /* unique index arbiter list (of
                                         * InferenceElem's) */
    Node               *arbiterWhere;   /* unique index arbiter WHERE clause */
    Oid                 constraint;     /* pg_constraint OID for arbiter */

    /* ON CONFLICT UPDATE */
    List               *onConflictSet;  /* List of ON CONFLICT SET TargetEntrys */
    Node               *onConflictWhere; /* qualifiers to restrict UPDATE to */
    int                 exclRelIndex;   /* RT index of 'excluded' relation */
    List               *exclRelTlist;   /* tlist of the EXCLUDED pseudo relation */
} OnConflictExpr;
```

## Detailed Description
OnConflictExpr is a specialized node type that represents PostgreSQL's ON CONFLICT clause functionality, allowing INSERT statements to handle unique constraint violations gracefully. This structure enables the implementation of "UPSERT" semantics where conflicts can either be ignored (DO NOTHING) or resolved through updates (DO UPDATE).

The structure is divided into two main functional areas: the arbiter mechanism and the conflict resolution actions. The arbiter determines which unique index or constraint is used to detect conflicts, while the action section specifies what to do when conflicts are detected.

The optimizer requires a list of inference elements and optionally a WHERE clause to infer the appropriate unique index. The inferred unique index (or occasionally multiple indexes) is used to arbitrate whether the alternative ON CONFLICT path should be taken. This inference mechanism is crucial for determining the correct conflict detection strategy.

For ON CONFLICT UPDATE scenarios, the structure maintains a list of SET target entries that specify which columns should be updated and how. The onConflictWhere clause provides additional filtering for when updates should occur. The EXCLUDED pseudo relation is represented through exclRelIndex and exclRelTlist, allowing references to the values that would have been inserted.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an OnConflictExpr node
- `action`: OnConflictAction enum specifying the conflict resolution action (DO NOTHING or DO UPDATE)
- `arbiterElems`: List of InferenceElem nodes that help identify the unique index for conflict detection
- `arbiterWhere`: Node containing WHERE clause conditions for unique index inference
- `constraint`: OID from pg_constraint catalog for the constraint being used as arbiter
- `onConflictSet`: List of TargetEntry nodes specifying SET clauses for ON CONFLICT UPDATE
- `onConflictWhere`: Node containing qualifiers that restrict when UPDATE should occur
- `exclRelIndex`: Range table index for the EXCLUDED pseudo relation
- `exclRelTlist`: Target list for the EXCLUDED pseudo relation, allowing access to conflicting values

## Dependencies
- Functions called/Symbols referenced:
  - OnConflictAction (enumeration for conflict actions)
  - NodeTag (for node identification)
  - List (for various element collections)
  - Node (for WHERE clause expressions)
  - Oid (for constraint identification)
- Called from (representative examples):
  - transformOnConflictClause (analyze.c:1129)
  - make_modifytable (createplan.c:7036)
  - create_modifytable_path (pathnode.c:3733)
  - infer_arbiter_indexes (plancat.c:707)
  - get_insert_query_def (ruleutils.c:6785)
  - rewriteTargetView (rewriteHandler.c:3709)

## Notes and Other Information
- Essential for implementing PostgreSQL's UPSERT functionality through ON CONFLICT clauses
- The arbiter mechanism allows flexible conflict detection based on unique indexes or constraints
- Supports both DO NOTHING and DO UPDATE conflict resolution strategies
- The EXCLUDED pseudo relation provides access to the values that would have been inserted
- Optimizer integration allows for efficient conflict detection and resolution planning
- Used in ModifyTable execution nodes for handling INSERT conflicts
- The constraint OID provides a direct link to the pg_constraint catalog for validation
- InferenceElem nodes in arbiterElems help the optimizer choose appropriate unique indexes
- Critical component in PostgreSQL's advanced INSERT statement processing