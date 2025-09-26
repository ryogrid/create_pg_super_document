# IndexStmt

## Location
[src/include/nodes/parsenodes.h:3348-3378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3348-L3378)

## Overview
IndexStmt represents the parsed form of SQL CREATE INDEX statements and related constraint creation operations, encapsulating all information needed to create indexes and associated constraints in PostgreSQL.

## Definition
```c
typedef struct IndexStmt
{
    NodeTag         type;
    char           *idxname;                    /* name of new index, or NULL for default */
    RangeVar       *relation;                  /* relation to build index on */
    char           *accessMethod;              /* name of access method (eg. btree) */
    char           *tableSpace;                /* tablespace, or NULL for default */
    List           *indexParams;               /* columns to index: a list of IndexElem */
    List           *indexIncludingParams;      /* additional columns to index: a list of IndexElem */
    List           *options;                   /* WITH clause options: a list of DefElem */
    Node           *whereClause;               /* qualification (partial-index predicate) */
    List           *excludeOpNames;            /* exclusion operator names, or NIL if none */
    char           *idxcomment;                /* comment to apply to index, or NULL */
    Oid             indexOid;                  /* OID of an existing index, if any */
    RelFileNumber   oldNumber;                 /* relfilenumber of existing storage, if any */
    SubTransactionId oldCreateSubid;           /* rd_createSubid of oldNumber */
    SubTransactionId oldFirstRelfilelocatorSubid; /* rd_firstRelfilelocatorSubid of oldNumber */
    bool            unique;                    /* is index unique? */
    bool            nulls_not_distinct;        /* null treatment for UNIQUE constraints */
    bool            primary;                   /* is index a primary key? */
    bool            isconstraint;              /* is it for a pkey/unique constraint? */
    bool            deferrable;                /* is the constraint DEFERRABLE? */
    bool            initdeferred;              /* is the constraint INITIALLY DEFERRED? */
    bool            transformed;               /* true when transformIndexStmt is finished */
    bool            concurrent;                /* should this be a concurrent index build? */
    bool            if_not_exists;             /* just do nothing if index already exists? */
    bool            reset_default_tblspc;      /* reset default_tablespace prior to executing */
} IndexStmt;
```

## Detailed Description
The IndexStmt structure is a comprehensive parse tree node that handles both index creation and constraint-related operations. It supports creating regular indexes, unique indexes, primary keys, and exclusion constraints. The structure can represent both new index creation and constraint creation using existing indexes. When isconstraint is true and indexOid is valid, it creates only a constraint entry without a new index. The statement supports various index types through access methods, partial indexes via WHERE clauses, and concurrent index builds for minimal locking.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an IndexStmt parse node
- `idxname`: Name for the new index, or NULL to auto-generate
- `relation`: RangeVar specifying the table to index
- `accessMethod`: String naming the index access method (btree, hash, gin, etc.)
- `tableSpace`: Tablespace name for index storage, or NULL for default
- `indexParams`: List of IndexElem structures defining indexed columns
- `indexIncludingParams`: List of IndexElem for non-key included columns
- `options`: List of DefElem structures for WITH clause options
- `whereClause`: Node representing partial index predicate condition
- `excludeOpNames`: List of exclusion operator names for exclusion constraints
- `idxcomment`: Comment text to associate with the index
- `indexOid`: OID of existing index when creating constraint only
- `oldNumber`: File number of existing storage for index reuse
- `oldCreateSubid`: Subtransaction ID for existing storage creation
- `oldFirstRelfilelocatorSubid`: Subtransaction ID for first relfilelocator
- `unique`: Boolean flag for unique index creation
- `nulls_not_distinct`: Whether NULL values are considered distinct in unique constraints
- `primary`: Boolean flag indicating primary key constraint
- `isconstraint`: Whether this creates a constraint entry
- `deferrable`: Whether constraint is deferrable
- `initdeferred`: Whether constraint is initially deferred
- `transformed`: Internal flag indicating transformation completion
- `concurrent`: Boolean flag for concurrent index building
- `if_not_exists`: Whether to skip if index already exists
- `reset_default_tblspc`: Whether to reset default tablespace setting

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar (table/relation reference)
  - RelFileNumber (relation file number type)
  - SubTransactionId (subtransaction identifier type)
  - NodeTag (parse node type identifier)
- Called from (representative examples):
  - DefineIndex (indexcmds.c:541)
  - ATExecAddIndex (tablecmds.c:9180)
  - transformIndexStmt (parse_utilcmd.c:2797)
  - ProcessUtilitySlow (utility.c:1454)

## Notes and Other Information
IndexStmt is one of the most complex DDL statement structures in PostgreSQL, supporting multiple index types and constraint scenarios. It's processed by DefineIndex() and integrates with the constraint system when isconstraint is true. The structure supports advanced features like partial indexes, included columns, concurrent builds, and index reuse for constraints. Proper handling requires coordination between the index creation system and constraint management.