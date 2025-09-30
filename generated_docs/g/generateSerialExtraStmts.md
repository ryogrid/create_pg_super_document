# generateSerialExtraStmts

## Location
[src/backend/parser/parse_utilcmd.c:361-561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L361-L561)

## Overview
Generates CREATE SEQUENCE and ALTER SEQUENCE ... OWNED BY statements to create and configure the sequence for a serial or identity column.

## Definition

```c
static void
generateSerialExtraStmts(CreateStmtContext *cxt, ColumnDef *column,
						 Oid seqtypid, List *seqoptions,
						 bool for_identity, bool col_exists,
						 char **snamespace_p, char **sname_p)
```
## Detailed Description
generateSerialExtraStmts is responsible for creating the sequence infrastructure needed for serial and identity columns in PostgreSQL. When a column is defined as SERIAL, BIGSERIAL, or has IDENTITY properties, this function generates the necessary SQL statements to:

1. Create the underlying sequence object with appropriate options
2. Set up the ownership relationship between the sequence and the column
3. Handle namespace resolution and name conflicts
4. Manage sequence persistence properties (logged/unlogged/temporary)

The function processes sequence options, filters out non-standard options, determines the sequence name (either user-specified or auto-generated), and creates the appropriate sequence commands. It handles both CREATE TABLE scenarios (where the column doesn't exist yet) and ALTER TABLE scenarios (where the column already exists).

For identity columns, special handling ensures the sequence is properly associated with the identity mechanism. The function also manages the execution order of statements, placing sequence creation before table creation and ownership assignment after.

## Parameters / Member Variables
- : CreateStmtContext containing parsing state and command lists
- : ColumnDef representing the serial/identity column
- : OID of the sequence data type (for typed sequences)
- : List of sequence options (START WITH, INCREMENT BY, etc.)
- : Boolean indicating if this is for an identity column
- : Boolean indicating if the column already exists (ALTER vs CREATE)
- : Output parameter for sequence namespace name (optional)
- : Output parameter for sequence name (optional)

## Dependencies
- Functions called/Symbols referenced:
  - [list_copy](../l/list_copy.md)
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - RelationGetNamespace
  - [RangeVarGetCreationNamespace](../R/RangeVarGetCreationNamespace.md)
  - [get_namespace_name](get_namespace_name.md)
  - [ChooseRelationName](../C/ChooseRelationName.md)
  - makeNode (CreateSeqStmt, AlterSeqStmt)
  - [makeRangeVar](../m/makeRangeVar.md)
  - [makeDefElem](../m/makeDefElem.md)
  - [makeTypeNameFromOid](../m/makeTypeNameFromOid.md)
  - list_make3
  - [makeString](../m/makeString.md)
- Called from (representative examples):
  - [transformColumnDefinition](../t/transformColumnDefinition.md)
  - [transformTableLikeClause](../t/transformTableLikeClause.md)
  - [transformAlterTableStmt](../t/transformAlterTableStmt.md)

## Notes and Other Information
The function handles several important edge cases: sequence name conflicts (though not guaranteed to be eliminated), persistence inheritance from the parent table, and proper ownership assignment for ALTER TABLE operations. The sequence name generation uses ChooseRelationName to minimize conflicts, but with very long column names, conflicts are still theoretically possible. The function carefully manages the execution order by placing CREATE SEQUENCE statements in the blist (before-table commands) and ALTER SEQUENCE OWNED BY statements in either blist or alist depending on whether the column already exists.

## Simplified Source

```c
static void generateSerialExtraStmts(CreateStmtContext *cxt, ColumnDef *column,
                                    Oid seqtypid, List *seqoptions,
                                    bool for_identity, bool col_exists,
                                    char **snamespace_p, char **sname_p) {
    DefElem *nameEl = NULL;
    DefElem *loggedEl = NULL;
    char *snamespace;
    char *sname;
    char seqpersistence;

    // Process sequence options, extracting special ones
    seqoptions = list_copy(seqoptions);
    foreach(option, seqoptions) {
        DefElem *defel = lfirst_node(DefElem, option);

        if (strcmp(defel->defname, "sequence_name") == 0) {
            nameEl = defel;
            seqoptions = foreach_delete_current(seqoptions, option);
        }
        else if (strcmp(defel->defname, "logged") == 0 ||
                 strcmp(defel->defname, "unlogged") == 0) {
            loggedEl = defel;
            seqoptions = foreach_delete_current(seqoptions, option);
        }
    }

    // Determine sequence namespace and name
    if (nameEl) {
        // Use user-specified name
        RangeVar *rv = makeRangeVarFromNameList(castNode(List, nameEl->arg));
        snamespace = rv->schemaname;
        if (!snamespace) {
            // Get namespace from relation context
            Oid snamespaceid = cxt->rel ?
                RelationGetNamespace(cxt->rel) :
                RangeVarGetCreationNamespace(cxt->relation);
            snamespace = get_namespace_name(snamespaceid);
        }
        sname = rv->relname;
    } else {
        // Generate sequence name automatically
        Oid snamespaceid = cxt->rel ?
            RelationGetNamespace(cxt->rel) :
            RangeVarGetCreationNamespace(cxt->relation);
        snamespace = get_namespace_name(snamespaceid);
        sname = ChooseRelationName(cxt->relation->relname,
                                  column->colname, "seq",
                                  snamespaceid, false);
    }

    // Determine sequence persistence
    seqpersistence = cxt->rel ?
        cxt->rel->rd_rel->relpersistence :
        cxt->relation->relpersistence;

    if (loggedEl) {
        if (strcmp(loggedEl->defname, "logged") == 0)
            seqpersistence = RELPERSISTENCE_PERMANENT;
        else
            seqpersistence = RELPERSISTENCE_UNLOGGED;
    }

    // Create the CREATE SEQUENCE statement
    CreateSeqStmt *seqstmt = makeNode(CreateSeqStmt);
    seqstmt->for_identity = for_identity;
    seqstmt->sequence = makeRangeVar(snamespace, sname, -1);
    seqstmt->sequence->relpersistence = seqpersistence;
    seqstmt->options = seqoptions;

    if (seqtypid)
        seqstmt->options = lcons(makeDefElem("as",
                                (Node *) makeTypeNameFromOid(seqtypid, -1), -1),
                                seqstmt->options);

    seqstmt->ownerId = cxt->rel ? cxt->rel->rd_rel->relowner : InvalidOid;
    cxt->blist = lappend(cxt->blist, seqstmt);

    // Store sequence name for identity columns
    column->identitySequence = seqstmt->sequence;

    // Create the ALTER SEQUENCE ... OWNED BY statement
    AlterSeqStmt *altseqstmt = makeNode(AlterSeqStmt);
    altseqstmt->sequence = makeRangeVar(snamespace, sname, -1);
    List *attnamelist = list_make3(makeString(snamespace),
                                  makeString(cxt->relation->relname),
                                  makeString(column->colname));
    altseqstmt->options = list_make1(makeDefElem("owned_by",
                                    (Node *) attnamelist, -1));
    altseqstmt->for_identity = for_identity;

    // Add to appropriate statement list based on column existence
    if (col_exists)
        cxt->blist = lappend(cxt->blist, altseqstmt);
    else
        cxt->alist = lappend(cxt->alist, altseqstmt);

    // Return sequence name components if requested
    if (snamespace_p) *snamespace_p = snamespace;
    if (sname_p) *sname_p = sname;
}
```