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