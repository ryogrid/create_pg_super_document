# transformOnConflictClause

## Location
[src/backend/parser/analyze.c:1118-1224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1118-L1224)

## Overview
Transforms an ON CONFLICT clause in an INSERT statement, handling both DO NOTHING and DO UPDATE variants with proper namespace management for the EXCLUDED pseudo-relation.

## Definition
```c
static OnConflictExpr *
transformOnConflictClause(ParseState *pstate, OnConflictClause *onConflictClause)
```

## Detailed Description
This function processes INSERT ... ON CONFLICT clauses to implement UPSERT functionality in PostgreSQL. It handles two main scenarios:

1. **ON CONFLICT DO NOTHING** - Simple conflict detection without any action
2. **ON CONFLICT DO UPDATE** - Complex conflict resolution with UPDATE expressions

Key processing steps include:
- Creating an EXCLUDED pseudo-relation RTE for DO UPDATE cases to reference conflicting values
- Processing arbiter elements (conflict detection columns/expressions/constraints)  
- Transforming UPDATE target list and WHERE clause for DO UPDATE actions
- Managing namespace visibility to allow EXCLUDED references in UPDATE expressions but not in RETURNING

The function carefully manages parse state to ensure UPDATE expressions are processed correctly within the INSERT context, temporarily switching p_is_insert to false during UPDATE expression processing.

Design considerations:
- EXCLUDED relation is marked as RELKIND_COMPOSITE_TYPE to bypass permission checks
- Namespace management ensures EXCLUDED is available in UPDATE expressions but removed before RETURNING processing
- Supports both column-based and constraint-based conflict detection

## Parameters / Member Variables
- `pstate`: Parse state containing namespace, target relation, and other context
- `onConflictClause`: Parsed ON CONFLICT clause containing action, target list, and arbiter information

## Dependencies
- Functions called/Symbols referenced:
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md) (creates EXCLUDED pseudo-relation)
  - [BuildOnConflictExcludedTargetlist](../B/BuildOnConflictExcludedTargetlist.md) (builds target list for EXCLUDED relation)
  - [transformOnConflictArbiter](transformOnConflictArbiter.md) (processes conflict detection elements)
  - [transformUpdateTargetList](transformUpdateTargetList.md) (handles DO UPDATE target expressions)
  - [transformWhereClause](transformWhereClause.md) (processes DO UPDATE WHERE condition)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)/list_delete_last (namespace management)

- Called from (representative examples):
  - [transformInsertStmt](transformInsertStmt.md) (main INSERT statement transformation)

## Notes and Other Information
- Critical for PostgreSQLs UPSERT functionality implementation
- The EXCLUDED pseudo-relation provides access to would-be-inserted values in UPDATE expressions
- Supports both implicit (column list) and explicit (constraint name) conflict detection
- Temporary modification of p_is_insert ensures proper expression context during UPDATE processing
- Careful namespace management prevents inappropriate EXCLUDED references in other clauses
- Returns OnConflictExpr node containing all necessary information for execution planning

## Simplified Source

```c
static OnConflictExpr *transformOnConflictClause(ParseState *pstate,
                                                 OnConflictClause *onConflictClause) {
    ParseNamespaceItem *exclNSItem = NULL;
    List *arbiterElems;
    Node *arbiterWhere;
    Oid arbiterConstraint;
    List *onConflictSet = NIL;
    Node *onConflictWhere = NULL;
    int exclRelIndex = 0;
    List *exclRelTlist = NIL;

    // For DO UPDATE, create EXCLUDED pseudo-relation
    if (onConflictClause->action == ONCONFLICT_UPDATE) {
        Relation targetrel = pstate->p_target_relation;

        // Add EXCLUDED pseudo-relation to range table
        exclNSItem = addRangeTableEntryForRelation(pstate, targetrel,
                                                  RowExclusiveLock,
                                                  makeAlias("excluded", NIL),
                                                  false, false);
        exclRelIndex = exclNSItem->p_rtindex;

        // Mark as composite type to bypass permission checks
        exclNSItem->p_rte->relkind = RELKIND_COMPOSITE_TYPE;

        // Build target list for EXPLAIN
        exclRelTlist = BuildOnConflictExcludedTargetlist(targetrel, exclRelIndex);
    }

    // Process arbiter clause (conflict detection)
    transformOnConflictArbiter(pstate, onConflictClause, &arbiterElems,
                              &arbiterWhere, &arbiterConstraint);

    // Process DO UPDATE action
    if (onConflictClause->action == ONCONFLICT_UPDATE) {
        // Switch to UPDATE context for expression processing
        pstate->p_is_insert = false;

        // Add EXCLUDED to namespace for UPDATE expressions
        addNSItemToQuery(pstate, exclNSItem, false, true, true);

        // Transform UPDATE target list and WHERE clause
        onConflictSet = transformUpdateTargetList(pstate, onConflictClause->targetList);
        onConflictWhere = transformWhereClause(pstate, onConflictClause->whereClause,
                                              EXPR_KIND_WHERE, "WHERE");

        // Remove EXCLUDED from namespace (not available in RETURNING)
        Assert((ParseNamespaceItem *) llast(pstate->p_namespace) == exclNSItem);
        pstate->p_namespace = list_delete_last(pstate->p_namespace);
    }

    // Build final OnConflictExpr node
    OnConflictExpr *result = makeNode(OnConflictExpr);
    result->action = onConflictClause->action;
    result->arbiterElems = arbiterElems;
    result->arbiterWhere = arbiterWhere;
    result->constraint = arbiterConstraint;
    result->onConflictSet = onConflictSet;
    result->onConflictWhere = onConflictWhere;
    result->exclRelIndex = exclRelIndex;
    result->exclRelTlist = exclRelTlist;

    return result;
}
```

**Key Points:**
- Transforms ON CONFLICT clauses for UPSERT functionality in INSERT statements
- Creates EXCLUDED pseudo-relation for DO UPDATE to access conflicting values
- Processes conflict detection (arbiter) elements and constraints
- Manages namespace carefully: EXCLUDED available in UPDATE expressions, not in RETURNING
- Temporarily switches parser context from INSERT to UPDATE for proper expression processing
- Returns OnConflictExpr containing all information needed for execution planning