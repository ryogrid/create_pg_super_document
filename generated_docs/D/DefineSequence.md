# DefineSequence

## Location
[src/backend/commands/sequence.c:121-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L121-L261)

## Overview
DefineSequence creates a new sequence relation in PostgreSQL, handling all aspects of sequence creation including relation setup, initial data population, and catalog registration.

## Definition

```c
enumber for the sequence;
```
## Detailed Description
DefineSequence is the main function responsible for creating PostgreSQL sequences. It performs comprehensive sequence creation by:

1. **Duplicate Check**: If  is specified, checks for existing sequences with the same name
2. **Parameter Validation**: Calls  to validate and set sequence options (start, increment, min, max, cache, cycle)
3. **Relation Creation**: Creates the underlying relation structure with three columns:
   -  (INT8): The last generated sequence value
   -  (INT8): Log counter for WAL optimization
   -  (BOOL): Whether the sequence has been used
4. **Data Initialization**: Populates the sequence with initial data using 
5. **Ownership Processing**: Handles  clauses if specified
6. **Catalog Registration**: Inserts sequence metadata into  system catalog

The function ensures atomicity and proper locking throughout the creation process.

## Parameters / Member Variables
- : ParseState for query parsing context and error reporting
- : CreateSeqStmt containing sequence definition including name, options, ownership, and if_not_exists flag

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)  
  - [init_params](../i/init_params.md)
  - [makeColumnDef](../m/makeColumnDef.md)
  - [DefineRelation](DefineRelation.md)
  - [sequence_open](../s/sequence_open.md)/sequence_close
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [fill_seq_with_data](../f/fill_seq_with_data.md)
  - [process_owned_by](../p/process_owned_by.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1667)

## Notes and Other Information
- Sequences are implemented as special relations with RELKIND_SEQUENCE
- The function handles both regular sequences and identity column sequences
- Extension membership is validated for CREATE IF NOT EXISTS operations for security
- The sequence relation uses AccessExclusiveLock during creation
- Initial sequence data is stored both in the relation and in pg_sequence catalog
- Supports all standard sequence options: START, INCREMENT, MINVALUE, MAXVALUE, CACHE, CYCLE, OWNED BY

## Simplified Source

```c
ObjectAddress DefineSequence(ParseState *pstate, CreateSeqStmt *seq) {
    FormData_pg_sequence seqform;
    FormData_pg_sequence_data seqdataform;
    bool need_seq_rewrite;
    List *owned_by;
    CreateStmt *stmt = makeNode(CreateStmt);
    Oid seqoid;
    ObjectAddress address;
    Relation rel;
    HeapTuple tuple;
    Datum value[SEQ_COL_LASTCOL], pgs_values[Natts_pg_sequence];
    bool null[SEQ_COL_LASTCOL], pgs_nulls[Natts_pg_sequence];

    // Handle IF NOT EXISTS case
    if (seq->if_not_exists) {
        RangeVarGetAndCheckCreationNamespace(seq->sequence, NoLock, &seqoid);
        if (OidIsValid(seqoid)) {
            ObjectAddressSet(address, RelationRelationId, seqoid);
            checkMembershipInCurrentExtension(&address);
            ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_TABLE),
                            errmsg("relation \"%s\" already exists, skipping",
                                   seq->sequence->relname)));
            return InvalidObjectAddress;
        }
    }

    // Validate and set sequence options
    init_params(pstate, seq->options, seq->for_identity, true,
                &seqform, &seqdataform, &need_seq_rewrite, &owned_by);

    // Create relation structure with three columns
    stmt->tableElts = NIL;
    for (int i = SEQ_COL_FIRSTCOL; i <= SEQ_COL_LASTCOL; i++) {
        ColumnDef *coldef = NULL;

        switch (i) {
            case SEQ_COL_LASTVAL:
                coldef = makeColumnDef("last_value", INT8OID, -1, InvalidOid);
                value[i - 1] = Int64GetDatumFast(seqdataform.last_value);
                break;
            case SEQ_COL_LOG:
                coldef = makeColumnDef("log_cnt", INT8OID, -1, InvalidOid);
                value[i - 1] = Int64GetDatum((int64) 0);
                break;
            case SEQ_COL_CALLED:
                coldef = makeColumnDef("is_called", BOOLOID, -1, InvalidOid);
                value[i - 1] = BoolGetDatum(false);
                break;
        }

        coldef->is_not_null = true;
        null[i - 1] = false;
        stmt->tableElts = lappend(stmt->tableElts, coldef);
    }

    // Set up relation creation parameters
    stmt->relation = seq->sequence;
    stmt->inhRelations = NIL;
    stmt->constraints = NIL;
    stmt->options = NIL;
    stmt->oncommit = ONCOMMIT_NOOP;
    stmt->tablespacename = NULL;
    stmt->if_not_exists = seq->if_not_exists;

    // Create the sequence relation
    address = DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId, NULL, NULL);
    seqoid = address.objectId;

    // Open sequence and initialize data
    rel = sequence_open(seqoid, AccessExclusiveLock);
    tuple = heap_form_tuple(RelationGetDescr(rel), value, null);
    fill_seq_with_data(rel, tuple);

    // Process OWNED BY clause if specified
    if (owned_by)
        process_owned_by(rel, owned_by, seq->for_identity);

    sequence_close(rel, NoLock);

    // Insert sequence metadata into pg_sequence catalog
    rel = table_open(SequenceRelationId, RowExclusiveLock);
    memset(pgs_nulls, 0, sizeof(pgs_nulls));

    pgs_values[Anum_pg_sequence_seqrelid - 1] = ObjectIdGetDatum(seqoid);
    pgs_values[Anum_pg_sequence_seqtypid - 1] = ObjectIdGetDatum(seqform.seqtypid);
    pgs_values[Anum_pg_sequence_seqstart - 1] = Int64GetDatumFast(seqform.seqstart);
    pgs_values[Anum_pg_sequence_seqincrement - 1] = Int64GetDatumFast(seqform.seqincrement);
    pgs_values[Anum_pg_sequence_seqmax - 1] = Int64GetDatumFast(seqform.seqmax);
    pgs_values[Anum_pg_sequence_seqmin - 1] = Int64GetDatumFast(seqform.seqmin);
    pgs_values[Anum_pg_sequence_seqcache - 1] = Int64GetDatumFast(seqform.seqcache);
    pgs_values[Anum_pg_sequence_seqcycle - 1] = BoolGetDatum(seqform.seqcycle);

    tuple = heap_form_tuple(RelationGetDescr(rel), pgs_values, pgs_nulls);
    CatalogTupleInsert(rel, tuple);

    heap_freetuple(tuple);
    table_close(rel, RowExclusiveLock);

    return address;
}
```