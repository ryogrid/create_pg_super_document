# ATPostAlterTypeParse

## Location
[src/backend/commands/tablecmds.c:14031-14246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L14031-L14246)

## Overview
ATPostAlterTypeParse re-parses previously saved definition strings for constraints, indexes, or statistics objects against new column data types and queues the resulting commands for execution.

## Definition
```c
static void ATPostAlterTypeParse(Oid oldId, Oid oldRelId, Oid refRelId, char *cmd, List **wqueue, LOCKMODE lockmode, bool rewrite)
```

## Detailed Description
This function handles the critical task of re-creating database objects (indexes, constraints, statistics) after column type changes. It parses the previously captured definition strings using the raw parser, then transforms them through appropriate parse utilities for different statement types. The function handles IndexStmt, AlterTableStmt, CreateStatsStmt, and AlterDomainStmt, converting them into work queue entries with modified subtypes for re-addition. It preserves object comments and handles special cases like foreign key constraint reuse and tablespace settings. The function also coordinates with RebuildConstraintComment to ensure constraint comments are properly restored.

## Parameters / Member Variables
- `oldId`: OID of the original object being rebuilt
- `oldRelId`: OID of the relation containing the object
- `refRelId`: OID of the referenced relation (for foreign keys)
- `cmd`: Previously saved definition string to re-parse
- `wqueue`: Double pointer to the ALTER TABLE work queue
- `lockmode`: Lock mode for the operations
- `rewrite`: Whether table rewrite is occurring (affects reuse optimizations)

## Dependencies
- Functions called/Symbols referenced:
  - [raw_parser](../r/raw_parser.md)
  - [transformIndexStmt](../t/transformIndexStmt.md)
  - [transformAlterTableStmt](../t/transformAlterTableStmt.md)
  - [transformStatsStmt](../t/transformStatsStmt.md)
  - [list_concat](../l/list_concat.md)
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - [TryReuseIndex](../T/TryReuseIndex.md)
  - [TryReuseForeignKey](../T/TryReuseForeignKey.md)
  - [GetComment](../G/GetComment.md)
  - [RebuildConstraintComment](../R/RebuildConstraintComment.md)
  - [get_constraint_index](../g/get_constraint_index.md)
  - makeNode
  - castNode
  - Various node types (IndexStmt, AlterTableStmt, CreateStatsStmt, etc.)
- Called from (representative examples):
  - [ATPostAlterTypeCleanup](ATPostAlterTypeCleanup.md) (multiple calls)
  - child_dependency_type

## Notes and Other Information
- Expects only ALTER TABLE and CREATE INDEX statements, bypassing normal query analysis
- Handles different statement types with appropriate transformation functions
- Preserves object comments by retrieving them before recreation
- Uses specialized subtypes (AT_ReAddIndex, AT_ReAddConstraint, etc.) for proper re-creation behavior
- Optimizes by trying to reuse existing objects when possible (TryReuseIndex, TryReuseForeignKey)
- Critical component of PostgreSQL's type change infrastructure for maintaining object consistency

## Simplified Source

```c
static void ATPostAlterTypeParse(Oid oldId, Oid oldRelId, Oid refRelId, char *cmd,
                                List **wqueue, LOCKMODE lockmode, bool rewrite) {
    List *raw_parsetree_list;
    List *querytree_list;
    ListCell *list_item;
    Relation rel;

    // Parse the saved definition string into raw parse trees
    raw_parsetree_list = raw_parser(cmd, RAW_PARSE_DEFAULT);
    querytree_list = NIL;

    // Transform each parsed statement based on its type
    foreach(list_item, raw_parsetree_list) {
        RawStmt *rs = lfirst_node(RawStmt, list_item);
        Node *stmt = rs->stmt;

        if (IsA(stmt, IndexStmt))
            querytree_list = lappend(querytree_list,
                                   transformIndexStmt(oldRelId, (IndexStmt *) stmt, cmd));
        else if (IsA(stmt, AlterTableStmt)) {
            List *beforeStmts, *afterStmts;
            stmt = (Node *) transformAlterTableStmt(oldRelId, (AlterTableStmt *) stmt,
                                                   cmd, &beforeStmts, &afterStmts);
            querytree_list = list_concat(querytree_list, beforeStmts);
            querytree_list = lappend(querytree_list, stmt);
            querytree_list = list_concat(querytree_list, afterStmts);
        }
        else if (IsA(stmt, CreateStatsStmt))
            querytree_list = lappend(querytree_list,
                                   transformStatsStmt(oldRelId, (CreateStatsStmt *) stmt, cmd));
        else
            querytree_list = lappend(querytree_list, stmt);
    }

    // Open relation for work queue operations
    rel = relation_open(oldRelId, NoLock);

    // Queue each transformed command for execution
    foreach(list_item, querytree_list) {
        Node *stm = (Node *) lfirst(list_item);
        AlteredTableInfo *tab = ATGetQueueEntry(wqueue, rel);

        if (IsA(stm, IndexStmt)) {
            IndexStmt *stmt = (IndexStmt *) stm;
            AlterTableCmd *newcmd;

            // Try to reuse existing index if not rewriting
            if (!rewrite)
                TryReuseIndex(oldId, stmt);

            stmt->reset_default_tblspc = true;
            stmt->idxcomment = GetComment(oldId, RelationRelationId, 0);

            // Queue index recreation
            newcmd = makeNode(AlterTableCmd);
            newcmd->subtype = AT_ReAddIndex;
            newcmd->def = (Node *) stmt;
            tab->subcmds[AT_PASS_OLD_INDEX] = lappend(tab->subcmds[AT_PASS_OLD_INDEX], newcmd);
        }
        else if (IsA(stm, AlterTableStmt)) {
            // Handle ALTER TABLE subcommands (constraints, etc.)
            AlterTableStmt *stmt = (AlterTableStmt *) stm;
            ListCell *lcmd;

            foreach(lcmd, stmt->cmds) {
                AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lcmd);

                if (cmd->subtype == AT_AddIndex) {
                    // Handle constraint indexes
                    IndexStmt *indstmt = castNode(IndexStmt, cmd->def);
                    Oid indoid = get_constraint_index(oldId);

                    if (!rewrite)
                        TryReuseIndex(indoid, indstmt);

                    indstmt->idxcomment = GetComment(indoid, RelationRelationId, 0);
                    indstmt->reset_default_tblspc = true;
                    cmd->subtype = AT_ReAddIndex;
                    tab->subcmds[AT_PASS_OLD_INDEX] = lappend(tab->subcmds[AT_PASS_OLD_INDEX], cmd);

                    RebuildConstraintComment(tab, AT_PASS_OLD_INDEX, oldId, rel, NIL, indstmt->idxname);
                }
                else if (cmd->subtype == AT_AddConstraint) {
                    // Handle regular constraints
                    Constraint *con = castNode(Constraint, cmd->def);

                    con->old_pktable_oid = refRelId;
                    if (con->contype == CONSTR_FOREIGN && !rewrite && tab->rewrite == 0)
                        TryReuseForeignKey(oldId, con);

                    con->reset_default_tblspc = true;
                    cmd->subtype = AT_ReAddConstraint;
                    tab->subcmds[AT_PASS_OLD_CONSTR] = lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);

                    RebuildConstraintComment(tab, AT_PASS_OLD_CONSTR, oldId, rel, NIL, con->conname);
                }
                // Skip AT_SetNotNull (handled automatically)
            }
        }
        else if (IsA(stm, CreateStatsStmt)) {
            // Handle statistics objects
            CreateStatsStmt *stmt = (CreateStatsStmt *) stm;
            AlterTableCmd *newcmd;

            stmt->stxcomment = GetComment(oldId, StatisticExtRelationId, 0);
            newcmd = makeNode(AlterTableCmd);
            newcmd->subtype = AT_ReAddStatistics;
            newcmd->def = (Node *) stmt;
            tab->subcmds[AT_PASS_MISC] = lappend(tab->subcmds[AT_PASS_MISC], newcmd);
        }
        // Handle other statement types...
    }

    relation_close(rel, NoLock);
}
```