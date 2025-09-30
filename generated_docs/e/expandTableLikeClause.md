# expandTableLikeClause

## Location
[src/backend/parser/parse_utilcmd.c:1169-1460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L1169-L1460)

## Overview
Processes LIKE clause options that require knowing the final column assignments in a newly created table, generating utility commands for indexes, constraints, defaults, and statistics after table creation.

## Definition
List *expandTableLikeClause(RangeVar *heapRel, TableLikeClause *table_like_clause)

## Detailed Description
This function executes after DefineRelation has been called for a new table and handles the post-creation processing of TABLE LIKE clauses. It analyzes the source table specified in the LIKE clause and generates a list of utility commands (ALTER TABLE, CREATE INDEX, CREATE STATISTICS, COMMENT) needed to replicate the requested features from the source table to the newly created table. The function maps attribute numbers between source and target tables and handles defaults, check constraints, indexes, extended statistics, and comments based on the specified LIKE options.

## Parameters / Member Variables
- `heapRel`: RangeVar specifying the newly created table that should receive the LIKE clause features
- `table_like_clause`: TableLikeClause containing the source table OID and option flags specifying which features to copy

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [relation_openrv](../r/relation_openrv.md)  
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
  - [TupleDescGetDefault](../T/TupleDescGetDefault.md)
  - [map_variable_attnos](../m/map_variable_attnos.md)
  - [stringToNode](../s/stringToNode.md)
  - [nodeToString](../n/nodeToString.md)
  - [GetComment](../G/GetComment.md)
  - [get_relation_constraint_oid](../g/get_relation_constraint_oid.md)
  - [generateClonedIndexStmt](../g/generateClonedIndexStmt.md)
  - [generateClonedExtStatsStmt](../g/generateClonedExtStatsStmt.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [RelationGetStatExtList](../R/RelationGetStatExtList.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Requires that transformTableLikeClause has already been called to validate and lock the source table
- Uses attribute mapping to ensure proper column correspondence between source and target tables
- Rejects whole-row table references in constraints and defaults to prevent future incompatibilities
- Generates commands in specific order: ALTER TABLE first, then indexes/statistics, then comments
- Maintains locks on both source and target tables throughout the operation
- Supports selective copying via option flags: DEFAULTS, GENERATED, CONSTRAINTS, INDEXES, STATISTICS, COMMENTS

## Simplified Source

```c
List *expandTableLikeClause(RangeVar *heapRel, TableLikeClause *table_like_clause)
{
    List *result = NIL;
    List *atsubcmds = NIL;
    Relation relation, childrel;
    TupleDesc tupleDesc;
    TupleConstr *constr;
    AttrMap *attmap;

    // Open the source relation (LIKE target)
    if (!OidIsValid(table_like_clause->relationOid))
        elog(ERROR, "expandTableLikeClause called on untransformed LIKE clause");

    relation = relation_open(table_like_clause->relationOid, NoLock);
    tupleDesc = RelationGetDescr(relation);
    constr = tupleDesc->constr;

    // Open the newly-created target table
    childrel = relation_openrv(heapRel, NoLock);

    // Build attribute mapping between source and target tables
    attmap = build_attrmap_by_name(RelationGetDescr(childrel), tupleDesc, false);

    // Process defaults and generated columns
    if ((table_like_clause->options &
         (CREATE_TABLE_LIKE_DEFAULTS | CREATE_TABLE_LIKE_GENERATED)) && constr != NULL) {

        for (AttrNumber parent_attno = 1; parent_attno <= tupleDesc->natts; parent_attno++) {
            Form_pg_attribute attribute = TupleDescAttr(tupleDesc, parent_attno - 1);

            // Skip dropped columns
            if (attribute->attisdropped)
                continue;

            // Copy defaults if requested and present
            if (attribute->atthasdef &&
                (attribute->attgenerated ?
                 (table_like_clause->options & CREATE_TABLE_LIKE_GENERATED) :
                 (table_like_clause->options & CREATE_TABLE_LIKE_DEFAULTS))) {

                Node *this_default = TupleDescGetDefault(tupleDesc, parent_attno);
                bool found_whole_row;

                AlterTableCmd *atsubcmd = makeNode(AlterTableCmd);
                atsubcmd->subtype = AT_CookedColumnDefault;
                atsubcmd->num = attmap->attnums[parent_attno - 1];
                atsubcmd->def = map_variable_attnos(this_default, 1, 0, attmap,
                                                  InvalidOid, &found_whole_row);

                // Reject whole-row references
                if (found_whole_row)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                   errmsg("cannot convert whole-row table reference")));

                atsubcmds = lappend(atsubcmds, atsubcmd);
            }
        }
    }

    // Process CHECK constraints
    if ((table_like_clause->options & CREATE_TABLE_LIKE_CONSTRAINTS) && constr != NULL) {
        for (int ccnum = 0; ccnum < constr->num_check; ccnum++) {
            char *ccname = constr->check[ccnum].ccname;
            char *ccbin = constr->check[ccnum].ccbin;
            bool ccnoinherit = constr->check[ccnum].ccnoinherit;
            Node *ccbin_node;
            bool found_whole_row;

            // Map attribute numbers in constraint expression
            ccbin_node = map_variable_attnos(stringToNode(ccbin), 1, 0, attmap,
                                           InvalidOid, &found_whole_row);

            // Reject whole-row references
            if (found_whole_row)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("cannot convert whole-row table reference")));

            // Create constraint definition
            Constraint *n = makeNode(Constraint);
            n->contype = CONSTR_CHECK;
            n->conname = pstrdup(ccname);
            n->location = -1;
            n->is_no_inherit = ccnoinherit;
            n->raw_expr = NULL;
            n->cooked_expr = nodeToString(ccbin_node);
            n->skip_validation = true;
            n->initially_valid = true;

            AlterTableCmd *atsubcmd = makeNode(AlterTableCmd);
            atsubcmd->subtype = AT_AddConstraint;
            atsubcmd->def = (Node *) n;
            atsubcmds = lappend(atsubcmds, atsubcmd);

            // Copy constraint comment if requested
            if ((table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS)) {
                char *comment = GetComment(get_relation_constraint_oid(RelationGetRelid(relation),
                                                                      n->conname, false),
                                         ConstraintRelationId, 0);
                if (comment != NULL) {
                    CommentStmt *stmt = makeNode(CommentStmt);
                    stmt->objtype = OBJECT_TABCONSTRAINT;
                    stmt->object = (Node *) list_make3(makeString(heapRel->schemaname),
                                                      makeString(heapRel->relname),
                                                      makeString(n->conname));
                    stmt->comment = comment;
                    result = lappend(result, stmt);
                }
            }
        }
    }

    // Create single ALTER TABLE command for all modifications
    if (atsubcmds) {
        AlterTableStmt *atcmd = makeNode(AlterTableStmt);
        atcmd->relation = copyObject(heapRel);
        atcmd->cmds = atsubcmds;
        atcmd->objtype = OBJECT_TABLE;
        atcmd->missing_ok = false;
        result = lcons(atcmd, result);
    }

    // Process indexes
    if ((table_like_clause->options & CREATE_TABLE_LIKE_INDEXES) &&
        relation->rd_rel->relhasindex) {

        List *parent_indexes = RelationGetIndexList(relation);
        ListCell *l;

        foreach(l, parent_indexes) {
            Oid parent_index_oid = lfirst_oid(l);
            Relation parent_index = index_open(parent_index_oid, AccessShareLock);

            // Generate CREATE INDEX statement
            IndexStmt *index_stmt = generateClonedIndexStmt(heapRel, parent_index,
                                                          attmap, NULL);

            // Copy index comment if requested
            if (table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) {
                char *comment = GetComment(parent_index_oid, RelationRelationId, 0);
                index_stmt->idxcomment = comment;
            }

            result = lappend(result, index_stmt);
            index_close(parent_index, AccessShareLock);
        }
    }

    // Process extended statistics
    if (table_like_clause->options & CREATE_TABLE_LIKE_STATISTICS) {
        List *parent_extstats = RelationGetStatExtList(relation);
        ListCell *l;

        foreach(l, parent_extstats) {
            Oid parent_stat_oid = lfirst_oid(l);

            // Generate CREATE STATISTICS statement
            CreateStatsStmt *stats_stmt = generateClonedExtStatsStmt(heapRel,
                                                                   RelationGetRelid(childrel),
                                                                   parent_stat_oid,
                                                                   attmap);

            // Copy statistics comment if requested
            if (table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) {
                char *comment = GetComment(parent_stat_oid, StatisticExtRelationId, 0);
                stats_stmt->stxcomment = comment;
            }

            result = lappend(result, stats_stmt);
        }

        list_free(parent_extstats);
    }

    // Close relations
    table_close(childrel, NoLock);
    table_close(relation, NoLock);

    return result;
}
```