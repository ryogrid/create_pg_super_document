# BuildDescForRelation

## Location
[src/backend/commands/tablecmds.c:1291-1392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L1291-L1392)

## Overview
BuildDescForRelation constructs a TupleDesc (tuple descriptor) from a list of ColumnDef nodes, defining the structure and attributes of a database relation.

## Definition

```c
struct dropmsgstrings *rentry;
```
## Detailed Description
BuildDescForRelation is responsible for converting a list of column definitions into a TupleDesc structure, which serves as PostgreSQL's internal representation of a relation's schema. The function iterates through each ColumnDef in the input list, extracting type information, performing permission checks, and initializing each attribute entry in the tuple descriptor. It handles various column properties including data types, collations, array dimensions, NOT NULL constraints, inheritance information, identity columns, generated columns, and storage attributes. The function also creates a TupleConstr structure when NOT NULL constraints are present.

## Parameters / Member Variables
- : List of ColumnDef structures representing the column definitions from a CREATE TABLE statement

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md)
  - [typenameTypeIdAndMod](../t/typenameTypeIdAndMod.md)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md)
  - [GetAttributeCompression](../G/GetAttributeCompression.md)
  - [GetAttributeStorage](../G/GetAttributeStorage.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)
  - [DefineVirtualRelation](../D/DefineVirtualRelation.md)

## Notes and Other Information
BuildDescForRelation performs comprehensive validation including type permission checks and array dimension limits (PG_INT16_MAX). It rejects SETOF column types as invalid for table definitions. The function sets up various attribute properties beyond basic type information, including local/inherited flags, identity and generated column settings, and compression/storage preferences. When any column has a NOT NULL constraint, it creates a TupleConstr structure to track constraint information. The resulting TupleDesc will require its tdtypeid field to be filled in later during relation creation.

## Simplified Source

```c
TupleDesc BuildDescForRelation(const List *columns) {
    int natts = list_length(columns);
    TupleDesc desc = CreateTemplateTupleDesc(natts);
    bool has_not_null = false;
    AttrNumber attnum = 0;

    // Process each column definition
    ListCell *l;
    foreach(l, columns) {
        ColumnDef *entry = lfirst(l);
        attnum++;

        // Get type information
        char *attname = entry->colname;
        Oid atttypid;
        int32 atttypmod;
        typenameTypeIdAndMod(NULL, entry->typeName, &atttypid, &atttypmod);

        // Check type permissions
        AclResult aclresult = object_aclcheck(TypeRelationId, atttypid, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, atttypid);

        // Get collation and array dimensions
        Oid attcollation = GetColumnDefCollation(NULL, entry, atttypid);
        int attdim = list_length(entry->typeName->arrayBounds);
        if (attdim > PG_INT16_MAX)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("too many array dimensions")));

        // Reject SETOF types
        if (entry->typeName->setof)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("column \"%s\" cannot be declared SETOF", attname)));

        // Initialize tuple descriptor entry
        TupleDescInitEntry(desc, attnum, attname, atttypid, atttypmod, attdim);
        Form_pg_attribute att = TupleDescAttr(desc, attnum - 1);

        // Set collation and additional properties
        TupleDescInitEntryCollation(desc, attnum, attcollation);
        att->attnotnull = entry->is_not_null;
        has_not_null |= entry->is_not_null;
        att->attislocal = entry->is_local;
        att->attinhcount = entry->inhcount;
        att->attidentity = entry->identity;
        att->attgenerated = entry->generated;
        att->attcompression = GetAttributeCompression(att->atttypid, entry->compression);

        // Set storage type
        if (entry->storage)
            att->attstorage = entry->storage;
        else if (entry->storage_name)
            att->attstorage = GetAttributeStorage(att->atttypid, entry->storage_name);
    }

    // Create constraint structure if needed
    if (has_not_null) {
        TupleConstr *constr = (TupleConstr *) palloc0(sizeof(TupleConstr));
        constr->has_not_null = true;
        constr->has_generated_stored = false;
        constr->defval = NULL;
        constr->missing = NULL;
        constr->num_defval = 0;
        constr->check = NULL;
        constr->num_check = 0;
        desc->constr = constr;
    } else {
        desc->constr = NULL;
    }

    return desc;
}
```