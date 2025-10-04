# AddRelationNewConstraints

## Location
[src/backend/catalog/heap.c:2314-2556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2314-L2556)

## Overview
AddRelationNewConstraints adds new column default expressions and/or constraint check expressions to an existing relation, processing both for efficiency and returning a list of cooked constraint representations.

## Definition

```c
union(NIL, vars);
```
## Detailed Description
AddRelationNewConstraints is a comprehensive function that processes new column defaults and check constraints for an existing relation. The function handles both types of constraints efficiently in a single operation, which is particularly useful during relation definition. It creates a ParseState to enable expression transformation, processes each constraint type appropriately, and returns a list of CookedConstraint nodes representing the processed constraints.

The function performs several key operations:
1. Sets up a ParseState with the target relation for expression transformation
2. Processes column default expressions, skipping NULL defaults unless they are generation expressions
3. Processes check constraints, handling both raw and pre-cooked expressions
4. Manages constraint naming, including automatic name generation and duplicate checking
5. Handles constraint merging when allowed
6. Updates the relation's constraint count in pg_class

The function is designed to handle both user-initiated constraint additions and internal system operations, with appropriate handling for inheritance scenarios.

## Parameters / Member Variables
- : The relation to be modified (must be opened with appropriate locking)
- : List of RawColumnDefault structures for new column defaults
- : List of Constraint nodes (only CONSTR_CHECK type will be processed)
- : Whether check constraints may be merged with existing ones
- : Whether the definition is local (true) or inherited (false)
- : Whether this is the result of an internal process, not a user request
- : Used during expression transformation of default values and CHECK constraints

## Dependencies
- Functions called/Symbols referenced:
  - [TupleConstr](../T/TupleConstr.md)
  - [make_parsestate](../m/make_parsestate.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
  - [cookDefault](../c/cookDefault.md)
  - [StoreAttrDefault](../S/StoreAttrDefault.md)
  - [cookConstraint](../c/cookConstraint.md)
  - [stringToNode](../s/stringToNode.md)
  - [MergeWithExistingConstraint](../M/MergeWithExistingConstraint.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [ChooseConstraintName](../C/ChooseConstraintName.md)
  - [StoreRelCheck](../S/StoreRelCheck.md)
  - [SetRelationNumChecks](../S/SetRelationNumChecks.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)
  - [ATExecAddColumn](ATExecAddColumn.md)
  - [ATExecColumnDefault](ATExecColumnDefault.md)
  - [ATAddCheckConstraint](ATAddCheckConstraint.md)

## Notes and Other Information
- Caller should hold an appropriate lock (typically AccessExclusiveLock or ShareUpdateExclusiveLock) until end of transaction
- Assumes caller has done CommandCounterIncrement if necessary to make relation's catalog tuples visible
- NULL column defaults are not stored explicitly unless they are generation expressions
- [Constraint](../C/Constraint.md) names are automatically generated if not provided, using "tab_col_check" for single-column constraints and "tab_check" for multi-column constraints
- The function updates the relation's check constraint count even if no changes were made to ensure SI update messages are sent
- Returns a list of CookedConstraint nodes showing the processed constraint information
- Domain type column defaults are always stored to override any domain defaults

## Simplified Source

```c
List *AddRelationNewConstraints(Relation rel, List *newColDefaults,
                                List *newConstraints, bool allow_merge,
                                bool is_local, bool is_internal,
                                const char *queryString) {
    List *cookedConstraints = NIL;
    TupleDesc tupleDesc = RelationGetDescr(rel);
    TupleConstr *oldconstr = tupleDesc->constr;
    int numoldchecks = oldconstr ? oldconstr->num_check : 0;
    int numchecks = numoldchecks;
    List *checknames = NIL;
    ParseState *pstate;
    ParseNamespaceItem *nsitem;

    // Set up ParseState for expression transformation
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;
    nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
                                           NULL, false, true);
    addNSItemToQuery(pstate, nsitem, true, true, true);

    // Process column default expressions
    foreach_ptr(RawColumnDefault, colDef, newColDefaults) {
        Form_pg_attribute atp = TupleDescAttr(rel->rd_att, colDef->attnum - 1);
        Node *expr = cookDefault(pstate, colDef->raw_default,
                                atp->atttypid, atp->atttypmod,
                                NameStr(atp->attname), atp->attgenerated);

        // Skip NULL defaults unless they are generation expressions
        if (expr == NULL || (!colDef->generated && IsA(expr, Const) &&
                            castNode(Const, expr)->constisnull)) {
            continue;
        }

        // Store the default and create cooked constraint
        Oid defOid = StoreAttrDefault(rel, colDef->attnum, expr,
                                      is_internal, false);
        CookedConstraint *cooked = palloc(sizeof(CookedConstraint));
        cooked->contype = CONSTR_DEFAULT;
        cooked->conoid = defOid;
        cooked->name = NULL;
        cooked->attnum = colDef->attnum;
        cooked->expr = expr;
        cooked->skip_validation = false;
        cooked->is_local = is_local;
        cooked->inhcount = is_local ? 0 : 1;
        cooked->is_no_inherit = false;
        cookedConstraints = lappend(cookedConstraints, cooked);
    }

    // Process check constraints
    foreach_node(Constraint, cdef, newConstraints) {
        if (cdef->contype != CONSTR_CHECK) {
            continue;
        }

        // Transform constraint expression
        Node *expr;
        if (cdef->raw_expr != NULL) {
            expr = cookConstraint(pstate, cdef->raw_expr,
                                 RelationGetRelationName(rel));
        } else {
            expr = stringToNode(cdef->cooked_expr);
        }

        // Handle constraint naming
        char *ccname;
        if (cdef->conname != NULL) {
            ccname = cdef->conname;
            // Check for name conflicts
            foreach_ptr(char, chkname, checknames) {
                if (strcmp(chkname, ccname) == 0) {
                    ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                                   errmsg("check constraint \"%s\" already exists",
                                          ccname)));
                }
            }

            // Try to merge with existing constraint if allowed
            if (MergeWithExistingConstraint(rel, ccname, expr, allow_merge,
                                           is_local, cdef->initially_valid,
                                           cdef->is_no_inherit)) {
                continue;
            }
        } else {
            // Generate constraint name based on referenced columns
            List *vars = pull_var_clause(expr, 0);
            vars = list_union(NIL, vars);

            char *colname = NULL;
            if (list_length(vars) == 1) {
                colname = get_attname(RelationGetRelid(rel),
                                     ((Var *) linitial(vars))->varattno, true);
            }

            ccname = ChooseConstraintName(RelationGetRelationName(rel),
                                         colname, "check",
                                         RelationGetNamespace(rel), checknames);
        }

        checknames = lappend(checknames, ccname);

        // Store the constraint
        Oid constrOid = StoreRelCheck(rel, ccname, expr, cdef->initially_valid,
                                      is_local, is_local ? 0 : 1,
                                      cdef->is_no_inherit, is_internal);
        numchecks++;

        // Create cooked constraint
        CookedConstraint *cooked = palloc(sizeof(CookedConstraint));
        cooked->contype = CONSTR_CHECK;
        cooked->conoid = constrOid;
        cooked->name = ccname;
        cooked->attnum = 0;
        cooked->expr = expr;
        cooked->skip_validation = cdef->skip_validation;
        cooked->is_local = is_local;
        cooked->inhcount = is_local ? 0 : 1;
        cooked->is_no_inherit = cdef->is_no_inherit;
        cookedConstraints = lappend(cookedConstraints, cooked);
    }

    // Update constraint count in pg_class
    SetRelationNumChecks(rel, numchecks);

    return cookedConstraints;
}
```