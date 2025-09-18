# AlteredTableInfo

## Location
src/backend/commands/tablecmds.c: 166 - 208

## Overview
AlteredTableInfo is a comprehensive structure that manages the state and metadata for table alteration operations in PostgreSQL. It tracks all phases of ALTER TABLE commands, from initial analysis through final execution and cleanup.

## Definition
```c
typedef struct AlteredTableInfo
{
    /* Information saved before any work commences: */
    Oid         relid;          /* Relation to work on */
    char        relkind;        /* Its relkind */
    TupleDesc   oldDesc;        /* Pre-modification tuple descriptor */

    /*
     * Transiently set during Phase 2, normally set to NULL.
     *
     * ATRewriteCatalogs sets this when it starts, and closes when ATExecCmd
     * returns control.  This can be exploited by ATExecCmd subroutines to
     * close/reopen across transaction boundaries.
     */
    Relation    rel;

    /* Information saved by Phase 1 for Phase 2: */
    List       *subcmds[AT_NUM_PASSES]; /* Lists of AlterTableCmd */
    /* Information saved by Phases 1/2 for Phase 3: */
    List       *constraints;        /* List of NewConstraint */
    List       *newvals;           /* List of NewColumnValue */
    List       *afterStmts;        /* List of utility command parsetrees */
    bool        verify_new_notnull; /* T if we should recheck NOT NULL */
    int         rewrite;           /* Reason for forced rewrite, if any */
    bool        chgAccessMethod;   /* T if SET ACCESS METHOD is used */
    Oid         newAccessMethod;   /* new access method; 0 means no change,
                                    * if above is true */
    Oid         newTableSpace;     /* new tablespace; 0 means no change */
    bool        chgPersistence;    /* T if SET LOGGED/UNLOGGED is used */
    char        newrelpersistence; /* if above is true */
    Expr       *partition_constraint; /* for attach partition validation */
    /* true, if validating default due to some other attach/detach */
    bool        validate_default;
    /* Objects to rebuild after completing ALTER TYPE operations */
    List       *changedConstraintOids; /* OIDs of constraints to rebuild */
    List       *changedConstraintDefs; /* string definitions of same */
    List       *changedIndexOids;      /* OIDs of indexes to rebuild */
    List       *changedIndexDefs;      /* string definitions of same */
    char       *replicaIdentityIndex;  /* index to reset as REPLICA IDENTITY */
    char       *clusterOnIndex;        /* index to use for CLUSTER */
    List       *changedStatisticsOids; /* OIDs of statistics to rebuild */
    List       *changedStatisticsDefs; /* string definitions of same */
} AlteredTableInfo;
```

## Detailed Description
AlteredTableInfo serves as the central coordination structure for PostgreSQL's multi-phase ALTER TABLE processing. It maintains state across the three main phases: Phase 1 (command preparation and validation), Phase 2 (catalog updates), and Phase 3 (table rewriting and cleanup). The structure tracks both the original table state and all pending changes, enabling complex table alterations to be performed atomically while handling dependencies, constraints, and related objects correctly.

## Parameters / Member Variables
- `relid`: Object identifier of the relation being altered
- `relkind`: The kind of relation (table, view, etc.) from pg_class.relkind
- `oldDesc`: Original tuple descriptor before any modifications begin
- `rel`: Temporarily opened relation during Phase 2 operations
- `subcmds[AT_NUM_PASSES]`: Array of command lists for different processing passes
- `constraints`: List of new constraints to be added (NewConstraint structures)
- `newvals`: List of new column values for table rewrite (NewColumnValue structures)
- `afterStmts`: List of utility commands to execute after main alterations
- `verify_new_notnull`: Flag indicating whether to revalidate NOT NULL constraints
- `rewrite`: Bitmask indicating reasons why a table rewrite is required
- `chgAccessMethod`: Flag indicating whether the access method is being changed
- `newAccessMethod`: OID of new access method (0 if unchanged)
- `newTableSpace`: OID of new tablespace (0 if unchanged)
- `chgPersistence`: Flag indicating whether persistence (LOGGED/UNLOGGED) is being changed
- `newrelpersistence`: New persistence setting if chgPersistence is true
- `partition_constraint`: Expression for validating partition attachment
- `validate_default`: Flag for validating defaults during partition operations
- `changedConstraintOids`: List of constraint OIDs that need rebuilding after ALTER TYPE
- `changedConstraintDefs`: String definitions for rebuilding constraints
- `changedIndexOids`: List of index OIDs that need rebuilding after ALTER TYPE
- `changedIndexDefs`: String definitions for rebuilding indexes
- `replicaIdentityIndex`: Index name to reset as REPLICA IDENTITY
- `clusterOnIndex`: Index name to use for CLUSTER operations
- `changedStatisticsOids`: List of statistics OIDs that need rebuilding
- `changedStatisticsDefs`: String definitions for rebuilding statistics

## Dependencies
- Functions called/Symbols referenced:
  - AT_NUM_PASSES (constant)
  - Various PostgreSQL core types (Oid, TupleDesc, List, Expr, etc.)
- Called from (representative examples):
  - [ATRewriteCatalogs](ATRewriteCatalogs.md)
  - [ATExecCmd](ATExecCmd.md)
  - [ATRewriteTables](ATRewriteTables.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - Various ATExec* functions for specific ALTER TABLE operations

## Notes and Other Information
- This structure is central to PostgreSQL's ALTER TABLE implementation and coordinates complex multi-step operations
- The three-phase approach allows for proper dependency handling and transactional safety
- The structure handles both simple alterations and complex operations requiring table rewrites
- Special handling for partition operations, constraint rebuilding, and index management
- Memory management and cleanup are handled by the ALTER TABLE subsystem