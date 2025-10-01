# ATExecAddColumn

## Location
[src/backend/commands/tablecmds.c:7012-7437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7012-L7437)

## Overview
Executes the actual addition of a column to a table, handling inheritance, type validation, default values, and catalog updates while managing complex scenarios like column merging and recursion.

## Definition
```c
static ObjectAddress ATExecAddColumn(List **wqueue, AlteredTableInfo *tab, Relation rel,
                                    AlterTableCmd **cmd, bool recurse, bool recursing,
                                    LOCKMODE lockmode, AlterTablePass cur_pass, 
                                    AlterTableUtilityContext *context)
```

## Detailed Description
This comprehensive function implements the core logic for adding columns to PostgreSQL tables. It handles numerous complex scenarios including inheritance hierarchies, type validation, default value processing, and catalog maintenance. The function operates in multiple phases and includes sophisticated logic for column merging in inheritance scenarios.

Key operations performed:
1. **Inheritance handling**: When adding to child tables, checks for existing columns with matching names and validates type compatibility, merging inheritance counts when appropriate
2. **Validation**: Prevents adding columns to partitions directly, enforces column name uniqueness, and validates type compatibility
3. **Catalog updates**: Updates pg_class and pg_attribute system catalogs with new column information
4. **Default value processing**: Handles various default value scenarios including identity columns, domain constraints, and missing value optimization
5. **Recursion management**: Recursively processes inheritance children while maintaining proper inheritance counts and avoiding infinite loops
6. **Dependency management**: Establishes proper dependencies for data types and collations

The function includes an optimization to avoid full table rewrites when possible by using PostgreSQL's "missing values" feature for default values that can be stored separately from table data.

## Parameters / Member Variables
- `wqueue`: Pointer to the ALTER TABLE work queue for managing related operations
- `tab`: Information about the table being altered, including rewrite requirements
- `rel`: The relation being modified
- `cmd`: Pointer to the ALTER TABLE command (may be modified during processing)
- `recurse`: Whether to apply changes to inheritance children
- `recursing`: Whether this is a recursive call (affects permission checks)
- `lockmode`: Lock mode to use for child relations
- `cur_pass`: Current phase of ALTER TABLE processing
- `context`: Context for command transformation and validation

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [ATSimplePermissions](ATSimplePermissions.md)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - [typenameTypeIdAndMod](../t/typenameTypeIdAndMod.md)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [check_for_column_name_collision](../c/check_for_column_name_collision.md)
  - [ATParseTransformCmd](ATParseTransformCmd.md)
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [CheckAttributeType](../C/CheckAttributeType.md)
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md)
  - [build_column_default](../b/build_column_default.md)
  - [DomainHasConstraints](../D/DomainHasConstraints.md)
  - [add_column_datatype_dependency](../a/add_column_datatype_dependency.md)
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - [ATExecAddColumn](ATExecAddColumn.md) (recursive calls)

## Notes and Other Information
- The function includes stack depth checking to prevent stack overflow during deep recursion
- Column merging logic ensures that inheritance counts are properly maintained across the hierarchy
- Identity columns have special handling and cannot be added recursively to tables with regular inheritance children
- The missing values optimization can avoid expensive table rewrites for non-volatile default expressions
- Partitions inherit identity columns but regular inheritance children do not
- The function maintains transactional consistency through appropriate use of CommandCounterIncrement()
- Error handling provides detailed messages for various failure scenarios including type mismatches and collation conflicts

## Simplified Source

```c
static ObjectAddress ATExecAddColumn(List **wqueue, AlteredTableInfo *tab, Relation rel,
                                    AlterTableCmd **cmd, bool recurse, bool recursing,
                                    LOCKMODE lockmode, AlterTablePass cur_pass,
                                    AlterTableUtilityContext *context) {
    Oid myrelid = RelationGetRelid(rel);
    ColumnDef *colDef = castNode(ColumnDef, (*cmd)->def);
    bool if_not_exists = (*cmd)->missing_ok;
    Relation attrdesc;
    int newattnum;
    ObjectAddress address;

    check_stack_depth();

    // Permission checks for recursive calls
    if (recursing) {
        ATSimplePermissions((*cmd)->subtype, rel, ATT_TABLE | ATT_FOREIGN_TABLE);
    }

    // Handle inheritance column merging
    if (colDef->inhcount > 0) {
        HeapTuple tuple = SearchSysCacheCopyAttName(myrelid, colDef->colname);
        if (HeapTupleIsValid(tuple)) {
            // Column exists in child - validate compatibility and merge
            Form_pg_attribute childatt = (Form_pg_attribute) GETSTRUCT(tuple);

            // Check type compatibility
            Oid ctypeId;
            int32 ctypmod;
            typenameTypeIdAndMod(NULL, colDef->typeName, &ctypeId, &ctypmod);
            if (ctypeId != childatt->atttypid || ctypmod != childatt->atttypmod) {
                ereport(ERROR, "child table has different type for column");
            }

            // Update inheritance count
            childatt->attinhcount++;
            CatalogTupleUpdate(attrdesc, &tuple->t_self, tuple);
            CommandCounterIncrement();
            return InvalidObjectAddress;
        }
    }

    // Check for column name collision
    if (!check_for_column_name_collision(rel, colDef->colname, if_not_exists)) {
        return InvalidObjectAddress;
    }

    // Transform command if needed
    if (context != NULL && !recursing) {
        *cmd = ATParseTransformCmd(wqueue, tab, rel, *cmd, recurse, lockmode, cur_pass, context);
        colDef = castNode(ColumnDef, (*cmd)->def);
    }

    // Open attribute catalog
    attrdesc = table_open(AttributeRelationId, RowExclusiveLock);

    // Determine new attribute number
    HeapTuple reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(myrelid));
    newattnum = ((Form_pg_class) GETSTRUCT(reltup))->relnatts + 1;

    if (newattnum > MaxHeapAttributeNumber) {
        ereport(ERROR, "tables can have at most %d columns", MaxHeapAttributeNumber);
    }

    // Build attribute descriptor
    TupleDesc tupdesc = BuildDescForRelation(list_make1(colDef));
    Form_pg_attribute attribute = TupleDescAttr(tupdesc, 0);
    attribute->attnum = newattnum;

    // Validate attribute type
    CheckAttributeType(NameStr(attribute->attname), attribute->atttypid,
                      attribute->attcollation, list_make1_oid(rel->rd_rel->reltype), 0);

    // Insert into pg_attribute
    InsertPgAttributeTuples(attrdesc, tupdesc, myrelid, NULL, NULL);
    table_close(attrdesc, RowExclusiveLock);

    // Update pg_class
    ((Form_pg_class) GETSTRUCT(reltup))->relnatts = newattnum;
    CatalogTupleUpdate(pgclass, &reltup->t_self, reltup);
    CommandCounterIncrement();

    // Handle default values
    if (colDef->raw_default) {
        RawColumnDefault *rawEnt = palloc(sizeof(RawColumnDefault));
        rawEnt->attnum = attribute->attnum;
        rawEnt->raw_default = copyObject(colDef->raw_default);
        rawEnt->generated = colDef->generated;

        AddRelationNewConstraints(rel, list_make1(rawEnt), NIL, false, true, false, NULL);
        CommandCounterIncrement();
    }

    // Setup default value for storage relations
    if (RELKIND_HAS_STORAGE(rel->rd_rel->relkind)) {
        Expr *defval = NULL;

        if (colDef->identity) {
            // Handle identity column default
            NextValueExpr *nve = makeNode(NextValueExpr);
            nve->seqid = RangeVarGetRelid(colDef->identitySequence, NoLock, false);
            nve->typeId = attribute->atttypid;
            defval = (Expr *) nve;
        } else {
            defval = (Expr *) build_column_default(rel, attribute->attnum);
        }

        if (defval) {
            // Try to optimize using missing values
            bool has_domain_constraints = DomainHasConstraints(attribute->atttypid);
            if (rel->rd_rel->relkind == RELKIND_RELATION &&
                !colDef->generated && !has_domain_constraints &&
                !contain_volatile_functions((Node *) defval)) {
                // Use missing value optimization
                StoreAttrMissingVal(rel, attribute->attnum, missingval);
            } else {
                // Require table rewrite
                tab->rewrite |= AT_REWRITE_DEFAULT_VAL;
            }

            // Add to newvals for phase 3
            NewColumnValue *newval = palloc0(sizeof(NewColumnValue));
            newval->attnum = attribute->attnum;
            newval->expr = expression_planner(defval);
            newval->is_generated = (colDef->generated != '\0');
            tab->newvals = lappend(tab->newvals, newval);
        }
    }

    // Add dependencies
    add_column_datatype_dependency(myrelid, newattnum, attribute->atttypid);
    add_column_collation_dependency(myrelid, newattnum, attribute->attcollation);

    // Recurse to children
    List *children = find_inheritance_children(RelationGetRelid(rel), lockmode);
    if (children && !recurse) {
        ereport(ERROR, "column must be added to child tables too");
    }

    foreach(child, children) {
        Oid childrelid = lfirst_oid(child);
        Relation childrel = table_open(childrelid, NoLock);
        AlteredTableInfo *childtab = ATGetQueueEntry(wqueue, childrel);

        // Recursive call
        ATExecAddColumn(wqueue, childtab, childrel, &childcmd, recurse, true,
                       lockmode, cur_pass, context);
        table_close(childrel, NoLock);
    }

    ObjectAddressSubSet(address, RelationRelationId, myrelid, newattnum);
    return address;
}
```