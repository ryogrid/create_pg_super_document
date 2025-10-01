# make_inh_translation_list

## Location
[src/backend/optimizer/util/appendinfo.c:80-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L80-L195)

## Overview
Builds the translation mapping between parent and child relation columns for inheritance hierarchies, creating both forward and reverse translation structures.

## Definition

```c
static void
make_inh_translation_list(Relation oldrelation, Relation newrelation,
						  Index newvarno,
						  AppendRelInfo *appinfo)
```
## Detailed Description
This function constructs the essential column mapping infrastructure needed for inheritance processing. It creates a list of Var nodes that translate parent table references to child table references, and a reverse-mapping array that maps child columns back to their parent equivalents. The function handles column name matching, type validation, and deals with dropped columns and schema differences between parent and child relations. It performs type and collation verification to ensure inheritance consistency.

## Parameters / Member Variables
- : The parent relation (source of the translation)
- : The child relation (target of the translation)  
- : Range table index for the new (child) relation
- : AppendRelInfo structure to populate with translation data

## Dependencies
- Functions called/Symbols referenced:
  - [makeVar](makeVar.md) (creates Var nodes for column references)
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) (looks up attributes by name)
  - RelationGetDescr (gets relation tuple descriptor)
  - TupleDescAttr (accesses tuple descriptor attributes)
  - [palloc0](../p/palloc0.md) (allocates zeroed memory)
- Called from (representative examples):
  - [make_append_rel_info](make_append_rel_info.md)

## Notes and Other Information
- Handles the special case where parent and child are the same relation (self-inheritance)
- Uses an optimization to check sequential columns first before falling back to syscache lookups
- Validates type and collation compatibility between matching parent-child columns
- Creates a reverse-translation array with 1-based indexing (0 means no match)
- Properly handles dropped columns by inserting NULL entries in the translation list

## Simplified Source

```c
static void
make_inh_translation_list(Relation oldrelation, Relation newrelation,
                          Index newvarno, AppendRelInfo *appinfo)
{
    List *vars = NIL;
    AttrNumber *pcolnos;
    TupleDesc old_tupdesc = RelationGetDescr(oldrelation);
    TupleDesc new_tupdesc = RelationGetDescr(newrelation);
    Oid new_relid = RelationGetRelid(newrelation);
    int oldnatts = old_tupdesc->natts;
    int newnatts = new_tupdesc->natts;
    int old_attno, new_attno = 0;

    // Initialize reverse-translation array
    appinfo->num_child_cols = newnatts;
    appinfo->parent_colnos = pcolnos = (AttrNumber *) palloc0(newnatts * sizeof(AttrNumber));

    // Process each parent column
    for (old_attno = 0; old_attno < oldnatts; old_attno++) {
        Form_pg_attribute att = TupleDescAttr(old_tupdesc, old_attno);

        if (att->attisdropped) {
            // Add NULL for dropped columns
            vars = lappend(vars, NULL);
            continue;
        }

        char *attname = NameStr(att->attname);
        Oid atttypid = att->atttypid;
        int32 atttypmod = att->atttypmod;
        Oid attcollation = att->attcollation;

        // Handle self-inheritance case
        if (oldrelation == newrelation) {
            vars = lappend(vars, makeVar(newvarno, (AttrNumber) (old_attno + 1),
                                        atttypid, atttypmod, attcollation, 0));
            pcolnos[old_attno] = old_attno + 1;
            continue;
        }

        // Find matching column in child relation
        if (new_attno >= newnatts ||
            (att = TupleDescAttr(new_tupdesc, new_attno))->attisdropped ||
            strcmp(attname, NameStr(att->attname)) != 0) {
            // Look up by name in system catalog
            HeapTuple newtup = SearchSysCacheAttName(new_relid, attname);
            if (!HeapTupleIsValid(newtup)) {
                elog(ERROR, "could not find inherited attribute \"%s\" of relation \"%s\"",
                     attname, RelationGetRelationName(newrelation));
            }
            new_attno = ((Form_pg_attribute) GETSTRUCT(newtup))->attnum - 1;
            ReleaseSysCache(newtup);
            att = TupleDescAttr(new_tupdesc, new_attno);
        }

        // Validate type and collation compatibility
        if (atttypid != att->atttypid || atttypmod != att->atttypmod) {
            elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's type",
                 attname, RelationGetRelationName(newrelation));
        }
        if (attcollation != att->attcollation) {
            elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's collation",
                 attname, RelationGetRelationName(newrelation));
        }

        // Create translation Var and update reverse mapping
        vars = lappend(vars, makeVar(newvarno, (AttrNumber) (new_attno + 1),
                                    atttypid, atttypmod, attcollation, 0));
        pcolnos[new_attno] = old_attno + 1;
        new_attno++;
    }

    appinfo->translated_vars = vars;
}
```