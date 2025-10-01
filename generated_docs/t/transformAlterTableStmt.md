# transformAlterTableStmt

## Location
[src/backend/parser/parse_utilcmd.c:3273-3636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L3273-L3636)

## Overview
Performs comprehensive parse analysis for ALTER TABLE statements, transforming various subcommands and generating additional statements needed before and after the main alteration.

## Definition
AlterTableStmt *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt, const char *queryString, List **beforeStmts, List **afterStmts)

## Detailed Description
The transformAlterTableStmt function handles the complex transformation of ALTER TABLE statements, which can involve multiple types of table modifications. Its comprehensive responsibilities include:

1. **Parse State and Context Setup**: Creates a ParseState for expression parsing and establishes a CreateStmtContext to manage the transformation process, handling both regular tables and foreign tables appropriately.

2. **Subcommand Processing**: Processes various ALTER TABLE subcommands including:
   - **AT_AddColumn**: Transforms new column definitions, processes constraints, and determines if foreign key validation can be skipped
   - **AT_AddConstraint**: Handles constraint additions and determines validation requirements
   - **AT_AlterColumnType**: Processes column type changes, transforms USING clauses, and handles identity column sequence updates
   - **AT_AddIdentity/AT_SetIdentity**: Manages identity column creation and modification, including associated sequence operations
   - **AT_AttachPartition/AT_DetachPartition**: Handles partition management operations

3. **Identity Column Handling**: For identity columns, generates appropriate ALTER SEQUENCE statements to maintain sequence consistency during type changes or identity modifications.

4. **Constraint Processing**: After processing subcommands, transforms various constraint types:
   - Index constraints via transformIndexConstraints
   - Foreign key constraints via transformFKConstraints  
   - Check constraints via transformCheckConstraints

5. **Index Management**: Processes index-creation commands, ensuring they are properly transformed through transformIndexStmt and converted to appropriate ALTER TABLE subcommands (AT_AddIndex or AT_AddIndexConstraint).

6. **Statement Orchestration**: Organizes the transformation results into three categories:
   - Commands that must execute before the main ALTER TABLE
   - The transformed ALTER TABLE statement itself
   - Commands that must execute after the main ALTER TABLE

The function ensures race condition safety by relying on the passed relid rather than the statement's relation field, and handles complex dependencies between different types of alterations.

## Parameters / Member Variables
- : Object identifier of the relation being altered
- : AlterTableStmt structure containing the parsed ALTER TABLE command to be transformed
- : Original SQL query string used for error reporting and expression transformation
- : Output parameter receiving list of statements to execute before the main ALTER TABLE
- : Output parameter receiving list of statements to execute after the main ALTER TABLE

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [make_parsestate](../m/make_parsestate.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
  - [transformColumnDefinition](transformColumnDefinition.md)
  - [transformTableConstraint](transformTableConstraint.md)
  - [transformExpr](transformExpr.md)
  - [get_attnum](../g/get_attnum.md)
  - [getIdentitySequence](../g/getIdentitySequence.md)
  - [typenameTypeId](typenameTypeId.md)
  - [generateSerialExtraStmts](../g/generateSerialExtraStmts.md)
  - [transformPartitionCmd](transformPartitionCmd.md)
  - [transformIndexConstraints](transformIndexConstraints.md)
  - [transformFKConstraints](transformFKConstraints.md)
  - [transformCheckConstraints](transformCheckConstraints.md)
  - [transformIndexStmt](transformIndexStmt.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [ATParseTransformCmd](../A/ATParseTransformCmd.md)
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)

## Notes and Other Information
- Central function for ALTER TABLE statement processing in PostgreSQL's utility command system
- Handles complex orchestration of multiple statement types that may be needed for a single ALTER TABLE
- Identity column support includes automatic sequence management for type changes
- Foreign key validation can be optimized when certain conditions are met (no new non-null defaults)
- The function distinguishes between regular tables and foreign tables for appropriate handling
- [Constraint](../C/Constraint.md) processing is deferred until all subcommands are initially processed
- Index statements generated from constraints are automatically transformed and integrated
- Race condition safety is maintained through consistent use of relid parameter
- The three-phase execution model (before/main/after) ensures proper dependency ordering
- Partition operations receive special handling through dedicated transformation functions
- The function is essential for maintaining data integrity during complex table modifications

## Simplified Source

```c
AlterTableStmt *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
                                       const char *queryString,
                                       List **beforeStmts, List **afterStmts) {
    Relation rel;
    ParseState *pstate;
    CreateStmtContext cxt;
    List *newcmds = NIL;
    bool skipValidation = true;

    // Open relation and setup parse state
    rel = relation_open(relid, NoLock);
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    // Add relation to parse state namespace
    ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(pstate, rel,
                                                              AccessShareLock, NULL, false, true);
    addNSItemToQuery(pstate, nsitem, false, true, true);

    // Initialize transformation context
    memset(&cxt, 0, sizeof(cxt));
    cxt.pstate = pstate;
    cxt.stmtType = (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) ?
                   "ALTER FOREIGN TABLE" : "ALTER TABLE";
    cxt.relation = stmt->relation;
    cxt.rel = rel;
    cxt.isalter = true;
    cxt.ispartitioned = (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

    // Transform each ALTER TABLE subcommand
    foreach(lcmd, stmt->cmds) {
        AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

        switch (cmd->subtype) {
            case AT_AddColumn:
                {
                    // Transform column definition and handle constraints
                    ColumnDef *def = castNode(ColumnDef, cmd->def);
                    transformColumnDefinition(&cxt, def);

                    if (def->raw_default != NULL) {
                        skipValidation = false;
                    }
                    def->constraints = NIL; // Processed separately
                    newcmds = lappend(newcmds, cmd);
                    break;
                }

            case AT_AddConstraint:
                // Transform table constraints
                if (IsA(cmd->def, Constraint)) {
                    transformTableConstraint(&cxt, (Constraint *) cmd->def);
                    if (((Constraint *) cmd->def)->contype == CONSTR_FOREIGN) {
                        skipValidation = false;
                    }
                }
                break;

            case AT_AlterColumnType:
                {
                    // Handle column type changes and identity sequences
                    ColumnDef *def = castNode(ColumnDef, cmd->def);

                    if (def->raw_default) {
                        def->cooked_default = transformExpr(pstate, def->raw_default,
                                                           EXPR_KIND_ALTER_COL_TRANSFORM);
                    }

                    // Handle identity column sequence updates
                    if (!RelationGetForm(rel)->relispartition) {
                        AttrNumber attnum = get_attnum(relid, cmd->name);
                        if (attnum > 0 && TupleDescAttr(RelationGetDescr(rel), attnum - 1)->attidentity) {
                            // Generate ALTER SEQUENCE statement for identity column
                            Oid seq_relid = getIdentitySequence(rel, attnum, false);
                            AlterSeqStmt *altseqstmt = makeNode(AlterSeqStmt);
                            // Setup sequence alteration...
                            cxt.blist = lappend(cxt.blist, altseqstmt);
                        }
                    }
                    newcmds = lappend(newcmds, cmd);
                    break;
                }

            case AT_AddIdentity:
                // Transform identity column addition
                generateSerialExtraStmts(&cxt, newdef, get_atttype(relid, attnum),
                                       def->options, true, true, NULL, NULL);
                newcmds = lappend(newcmds, cmd);
                break;

            case AT_AttachPartition:
            case AT_DetachPartition:
                // Transform partition commands
                transformPartitionCmd(&cxt, (PartitionCmd *) cmd->def);
                newcmds = lappend(newcmds, cmd);
                break;

            default:
                // Pass through other subcommands unchanged
                newcmds = lappend(newcmds, cmd);
                break;
        }
    }

    // Process constraints after all subcommands
    transformIndexConstraints(&cxt);
    transformFKConstraints(&cxt, skipValidation, true);
    transformCheckConstraints(&cxt, false);

    // Convert index statements to ALTER TABLE subcommands
    foreach(l, cxt.alist) {
        Node *istmt = lfirst(l);
        if (IsA(istmt, IndexStmt)) {
            IndexStmt *idxstmt = transformIndexStmt(relid, (IndexStmt *) istmt, queryString);
            AlterTableCmd *newcmd = makeNode(AlterTableCmd);
            newcmd->subtype = OidIsValid(idxstmt->indexOid) ?
                             AT_AddIndexConstraint : AT_AddIndex;
            newcmd->def = (Node *) idxstmt;
            newcmds = lappend(newcmds, newcmd);
        }
    }

    // Add constraint commands
    foreach(l, cxt.ckconstraints) {
        AlterTableCmd *newcmd = makeNode(AlterTableCmd);
        newcmd->subtype = AT_AddConstraint;
        newcmd->def = (Node *) lfirst_node(Constraint, l);
        newcmds = lappend(newcmds, newcmd);
    }

    relation_close(rel, NoLock);

    // Return transformed statement with before/after lists
    stmt->cmds = newcmds;
    *beforeStmts = cxt.blist;
    *afterStmts = cxt.alist;

    return stmt;
}
```