# InsertRule

## Location
[src/backend/rewrite/rewriteDefine.c:52-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L52-L189)

## Overview
InsertRule is a static function that inserts a rewrite rule definition into the PostgreSQL system catalog pg_rewrite, handling both new rule creation and rule replacement scenarios.

## Definition

```c
static Oid
InsertRule(const char *rulname,
		   int evtype,
		   Oid eventrel_oid,
		   bool evinstead,
		   Node *event_qual,
		   List *action,
		   bool replace)
```
## Detailed Description
InsertRule performs the low-level catalog insertion of a rewrite rule into the pg_rewrite system table. It constructs the appropriate catalog tuple from the provided rule parameters, handles rule replacement logic when a rule with the same name already exists on the same relation, and establishes proper dependency relationships. The function converts the rule's qualification and action trees to string representations for storage and manages both new insertions and updates of existing rules based on the replace parameter.

## Parameters / Member Variables
- : The name of the rule to be created
- : The event type that triggers the rule (SELECT, INSERT, UPDATE, DELETE)
- : The OID of the relation the rule is defined on
- : Boolean indicating if this is an INSTEAD rule
- : The qualification condition for when the rule fires (can be NULL)
- : List of action statements to execute when the rule fires
- : Boolean indicating whether to replace an existing rule with the same name

## Dependencies
- Functions called/Symbols referenced:
  - [nodeToString](../n/nodeToString.md)
  - [namestrcpy](../n/namestrcpy.md)
  - [NameGetDatum](../N/NameGetDatum.md)
  - [CharGetDatum](../C/CharGetDatum.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnExpr](../r/recordDependencyOnExpr.md)
  - [getInsertSelectQuery](../g/getInsertSelectQuery.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [DefineQueryRewrite](../D/DefineQueryRewrite.md)

## Notes and Other Information
- This is a static function internal to rewriteDefine.c
- Handles rule replacement by checking for existing rules and either throwing an error or updating the existing tuple
- Establishes dependency relationships to ensure proper cleanup when related objects are dropped
- Uses different dependency types: DEPENDENCY_INTERNAL for SELECT rules (prevents deletion of view's SELECT rule), DEPENDENCY_AUTO for other rule types
- Records dependencies on objects referenced in both the rule action and qualification expressions
- Returns the OID of the newly created or updated rewrite rule

## Simplified Source

```c
static Oid InsertRule(const char *rulname, int evtype, Oid eventrel_oid,
                     bool evinstead, Node *event_qual, List *action, bool replace) {
    // Convert rule components to string representation for storage
    char *evqual = nodeToString(event_qual);
    char *actiontree = nodeToString((Node *) action);

    // Set up catalog tuple values
    Datum values[Natts_pg_rewrite];
    bool nulls[Natts_pg_rewrite] = {0};
    NameData rname;

    namestrcpy(&rname, rulname);
    values[Anum_pg_rewrite_rulename - 1] = NameGetDatum(&rname);
    values[Anum_pg_rewrite_ev_class - 1] = ObjectIdGetDatum(eventrel_oid);
    values[Anum_pg_rewrite_ev_type - 1] = CharGetDatum(evtype + '0');
    values[Anum_pg_rewrite_ev_enabled - 1] = CharGetDatum(RULE_FIRES_ON_ORIGIN);
    values[Anum_pg_rewrite_is_instead - 1] = BoolGetDatum(evinstead);
    values[Anum_pg_rewrite_ev_qual - 1] = CStringGetTextDatum(evqual);
    values[Anum_pg_rewrite_ev_action - 1] = CStringGetTextDatum(actiontree);

    // Open pg_rewrite catalog
    Relation pg_rewrite_desc = table_open(RewriteRelationId, RowExclusiveLock);

    // Check for existing rule with same name
    HeapTuple oldtup = SearchSysCache2(RULERELNAME,
                                      ObjectIdGetDatum(eventrel_oid),
                                      PointerGetDatum(rulname));

    Oid rewriteObjectId;
    bool is_update = false;
    HeapTuple tup;

    if (HeapTupleIsValid(oldtup)) {
        // Handle rule replacement
        if (!replace)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("rule \"%s\" for relation \"%s\" already exists",
                                  rulname, get_rel_name(eventrel_oid))));

        // Update existing rule
        bool replaces[Natts_pg_rewrite] = {0};
        replaces[Anum_pg_rewrite_ev_type - 1] = true;
        replaces[Anum_pg_rewrite_is_instead - 1] = true;
        replaces[Anum_pg_rewrite_ev_qual - 1] = true;
        replaces[Anum_pg_rewrite_ev_action - 1] = true;

        tup = heap_modify_tuple(oldtup, RelationGetDescr(pg_rewrite_desc),
                               values, nulls, replaces);
        CatalogTupleUpdate(pg_rewrite_desc, &tup->t_self, tup);

        rewriteObjectId = ((Form_pg_rewrite) GETSTRUCT(tup))->oid;
        is_update = true;
        ReleaseSysCache(oldtup);
    } else {
        // Create new rule
        rewriteObjectId = GetNewOidWithIndex(pg_rewrite_desc,
                                           RewriteOidIndexId,
                                           Anum_pg_rewrite_oid);
        values[Anum_pg_rewrite_oid - 1] = ObjectIdGetDatum(rewriteObjectId);

        tup = heap_form_tuple(pg_rewrite_desc->rd_att, values, nulls);
        CatalogTupleInsert(pg_rewrite_desc, tup);
    }

    heap_freetuple(tup);

    // Handle dependencies
    if (is_update)
        deleteDependencyRecordsFor(RewriteRelationId, rewriteObjectId, false);

    // Create dependency on the relation (INTERNAL for SELECT rules, AUTO for others)
    ObjectAddress myself = {RewriteRelationId, rewriteObjectId, 0};
    ObjectAddress referenced = {RelationRelationId, eventrel_oid, 0};

    recordDependencyOn(&myself, &referenced,
                      (evtype == CMD_SELECT) ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO);

    // Record dependencies on referenced objects
    recordDependencyOnExpr(&myself, (Node *) action, NIL, DEPENDENCY_NORMAL);

    if (event_qual != NULL) {
        Query *qry = linitial_node(Query, action);
        qry = getInsertSelectQuery(qry, NULL);
        recordDependencyOnExpr(&myself, event_qual, qry->rtable, DEPENDENCY_NORMAL);
    }

    // Post creation hook and cleanup
    InvokeObjectPostCreateHook(RewriteRelationId, rewriteObjectId, 0);
    table_close(pg_rewrite_desc, RowExclusiveLock);

    return rewriteObjectId;
}
```