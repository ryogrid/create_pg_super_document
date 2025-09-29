# MergeAttributes

## Location
[src/backend/commands/tablecmds.c:2470-3051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L2470-L3051)

## Overview
A comprehensive function that merges column definitions from parent tables with explicitly declared columns to create a complete schema for table inheritance or partitioning.

## Definition
static List *MergeAttributes(List *columns, const List *supers, char relpersistence, bool is_partition, List **supconstr)

## Detailed Description
This function is the core engine for PostgreSQL's table inheritance and partitioning system. It takes column definitions from multiple parent tables and merges them with any explicitly declared columns to produce a unified schema. The function handles complex inheritance scenarios including:

1. **Multi-level inheritance**: Processes parent tables in left-to-right order, preserving inheritance hierarchy
2. **Column conflict resolution**: Merges columns with identical names, ensuring type compatibility and resolving default value conflicts
3. **Constraint inheritance**: Inherits CHECK constraints from parents, adjusting column references appropriately
4. **Default value handling**: Implements sophisticated rules for inheriting and overriding default values and generation expressions
5. **Partition-specific logic**: Special handling for partitioned tables with different validation rules

The function maintains PostgreSQL's inheritance semantics where attributes appear in the order of first definition across the inheritance hierarchy. It validates persistence constraints, ownership permissions, and relationship types throughout the process.

## Parameters / Member Variables
- columns: List of ColumnDef structures representing explicitly declared columns (destructively modified)
- supers: List of OIDs representing parent relation identifiers (already locked by caller)
- relpersistence: Character indicating table persistence type (permanent, temporary, etc.)
- is_partition: Boolean flag indicating whether this is a partition operation vs regular inheritance
- supconstr: Output parameter receiving list of inherited constraints from parents

## Dependencies
- Functions called/Symbols referenced:
  - [makeColumnDef](../m/makeColumnDef.md) (creates column definition structures)
  - [findAttrByName](../f/findAttrByName.md) (locates columns by name in inheritance hierarchy)
  - [MergeInheritedAttribute](MergeInheritedAttribute.md) (merges column definitions from multiple parents)
  - [MergeChildAttribute](MergeChildAttribute.md) (merges child and inherited column definitions)
  - [MergeCheckConstraint](MergeCheckConstraint.md) (merges inherited CHECK constraints)
  - [make_attrmap](../m/make_attrmap.md)/free_attrmap (manages attribute number mapping)
  - [map_variable_attnos](../m/map_variable_attnos.md) (adjusts variable references in expressions)
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md) (validates table availability for operations)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (main table creation function)

## Notes and Other Information
- This is a static function within tablecmds.c, central to PostgreSQL's DDL processing
- The function is destructive - it modifies the input columns list during processing
- Implements PostgreSQL's complex inheritance rules including attribute ordering, default value precedence, and constraint merging
- Handles both regular inheritance and partitioning with different validation rules for each
- Performs extensive validation including column limits (MaxHeapAttributeNumber), type compatibility, and permission checks
- The algorithm is O(n^2) in column count but optimized for typical table sizes
- Critical for maintaining PostgreSQL's inheritance semantics and ensuring schema consistency across table hierarchies

## Simplified Source

```c
static List *
MergeAttributes(List *columns, const List *supers, char relpersistence,
                bool is_partition, List **supconstr)
{
    List       *inh_columns = NIL;
    List       *constraints = NIL;
    bool        have_bogus_defaults = false;
    int         child_attno;

    // Check column count limits
    if (list_length(columns) > MaxHeapAttributeNumber)
        ereport(ERROR, "tables can have at most %d columns");

    // Check for duplicate column names in explicit list
    for (int coldefpos = 0; coldefpos < list_length(columns); coldefpos++) {
        ColumnDef *coldef = list_nth_node(ColumnDef, columns, coldefpos);

        // Check for duplicates in remaining columns
        for (int restpos = coldefpos + 1; restpos < list_length(columns);) {
            ColumnDef *restdef = list_nth_node(ColumnDef, columns, restpos);

            if (strcmp(coldef->colname, restdef->colname) == 0) {
                if (coldef->is_from_type) {
                    // Merge column options from type definition
                    merge_column_options(coldef, restdef);
                    columns = list_delete_nth_cell(columns, restpos);
                } else {
                    ereport(ERROR, "column specified more than once");
                }
            } else {
                restpos++;
            }
        }
    }

    // For partitions, save column constraints for later processing
    if (is_partition) {
        saved_columns = columns;
        columns = NIL;
    }

    // Process each parent table left-to-right
    child_attno = 0;
    foreach(lc, supers) {
        Oid parent = lfirst_oid(lc);
        Relation relation = table_open(parent, NoLock);
        TupleDesc tupleDesc = RelationGetDescr(relation);

        // Validate parent table compatibility
        validate_parent_relation(relation, is_partition, relpersistence);

        // Create attribute mapping for this parent
        AttrMap *newattmap = make_attrmap(tupleDesc->natts);

        // Process each attribute from parent
        for (AttrNumber parent_attno = 1; parent_attno <= tupleDesc->natts; parent_attno++) {
            Form_pg_attribute attribute = TupleDescAttr(tupleDesc, parent_attno - 1);

            if (attribute->attisdropped)
                continue;

            // Create new column definition from parent attribute
            ColumnDef *newdef = makeColumnDef(attribute->attname, attribute->atttypid,
                                            attribute->atttypmod, attribute->attcollation);
            copy_attribute_properties(newdef, attribute, is_partition);

            // Check if column already exists from previous parent
            int exist_attno = findAttrByName(attribute->attname, inh_columns);
            if (exist_attno > 0) {
                // Merge with existing column definition
                ColumnDef *mergeddef = MergeInheritedAttribute(inh_columns, exist_attno, newdef);
                newattmap->attnums[parent_attno - 1] = exist_attno;
            } else {
                // Add as new inherited column
                newdef->inhcount = 1;
                newdef->is_local = false;
                inh_columns = lappend(inh_columns, newdef);
                newattmap->attnums[parent_attno - 1] = ++child_attno;
            }

            // Handle default expressions and constraints
            if (attribute->atthasdef) {
                process_inherited_default(attribute, newdef, newattmap, relation);
            }
        }

        // Inherit CHECK constraints from parent
        inherit_check_constraints(relation->rd_desc->constr, newattmap, &constraints);

        free_attrmap(newattmap);
        table_close(relation, NoLock);
    }

    // Merge explicitly declared columns with inherited columns
    if (inh_columns != NIL) {
        merge_explicit_columns(columns, inh_columns, is_partition);
        columns = inh_columns;

        // Check final column count
        if (list_length(columns) > MaxHeapAttributeNumber)
            ereport(ERROR, "tables can have at most %d columns");
    }

    // Process partition-specific column constraints
    if (is_partition) {
        process_partition_column_constraints(saved_columns, columns);
    }

    // Check for unresolved default value conflicts
    if (have_bogus_defaults) {
        validate_default_conflicts(columns);
    }

    *supconstr = constraints;
    return columns;
}
```