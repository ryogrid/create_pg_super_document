# AlterFunction

## Location
[src/backend/commands/functioncmds.c:1343-1520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1343-L1520)

## Overview
Implements the ALTER FUNCTION utility command, allowing modification of function properties such as volatility, strictness, security, cost, and parallel execution settings.

## Definition

```c
ObjectAddress
AlterFunction(ParseState *pstate, AlterFunctionStmt *stmt)
```
## Detailed Description
AlterFunction processes ALTER FUNCTION statements to modify various attributes of existing functions or procedures. The function validates permissions, processes the requested changes, and updates the pg_proc catalog accordingly. Key capabilities include:

1. Permission validation - ensures the user owns the function
2. Function type validation - prevents altering aggregates 
3. Attribute processing - handles volatility, strictness, security definer, leakproof, cost, rows, support functions, and parallel execution settings
4. Configuration parameter updates - processes SET/RESET clauses for function-specific GUC settings
5. Dependency management - properly handles support function dependencies
6. Catalog updates - commits changes to the pg_proc system catalog

The function handles both regular functions and procedures, with appropriate validation for procedure-specific constraints.

## Parameters / Member Variables
- : Parse state containing parsing context and environment information
- : AlterFunctionStmt structure containing the function identifier and list of requested modifications

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - ObjectAddressSet
  - SearchSysCacheCopy1
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [compute_common_attribute](../c/compute_common_attribute.md)
  - [interpret_func_volatility](../i/interpret_func_volatility.md)
  - [interpret_func_support](../i/interpret_func_support.md)
  - [interpret_func_parallel](../i/interpret_func_parallel.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [update_proconfig_value](../u/update_proconfig_value.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility.c:1659)

## Notes and Other Information
- Excludes RENAME and OWNER operations which are handled by the generic ALTER framework
- Enforces superuser privilege requirement for leakproof function designation
- Validates cost and rows parameters for positive values
- Properly manages support function dependencies with changeDependencyFor/recordDependencyOn
- Uses heap_modify_tuple for efficient catalog updates when handling configuration parameters
- Invokes post-alter hooks for proper event trigger and extension handling
- The function comment warns against accessing procForm after heap_modify_tuple as it becomes a dangling pointer

## Simplified Source

```c
ObjectAddress AlterFunction(ParseState *pstate, AlterFunctionStmt *stmt) {
    HeapTuple tup;
    Oid funcOid;
    Form_pg_proc procForm;
    bool is_procedure;
    Relation rel;
    ListCell *l;

    // Variable declarations for parsed attributes
    DefElem *volatility_item = NULL;
    DefElem *strict_item = NULL;
    DefElem *security_def_item = NULL;
    DefElem *leakproof_item = NULL;
    List *set_items = NIL;
    DefElem *cost_item = NULL;
    DefElem *rows_item = NULL;
    DefElem *support_item = NULL;
    DefElem *parallel_item = NULL;
    ObjectAddress address;

    // Open procedure catalog and lookup function
    rel = table_open(ProcedureRelationId, RowExclusiveLock);
    funcOid = LookupFuncWithArgs(stmt->objtype, stmt->func, false);
    ObjectAddressSet(address, ProcedureRelationId, funcOid);

    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for function %u", funcOid);

    procForm = (Form_pg_proc) GETSTRUCT(tup);

    // Permission check - must own function
    if (!object_ownercheck(ProcedureRelationId, funcOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, stmt->objtype,
                       NameListToString(stmt->func->objname));

    // Prevent altering aggregates
    if (procForm->prokind == PROKIND_AGGREGATE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("\"%s\" is an aggregate function",
                              NameListToString(stmt->func->objname))));

    is_procedure = (procForm->prokind == PROKIND_PROCEDURE);

    // Parse all requested actions
    foreach(l, stmt->actions) {
        DefElem *defel = (DefElem *) lfirst(l);

        if (!compute_common_attribute(pstate, is_procedure, defel,
                                     &volatility_item, &strict_item,
                                     &security_def_item, &leakproof_item,
                                     &set_items, &cost_item, &rows_item,
                                     &support_item, &parallel_item))
            elog(ERROR, "option \"%s\" not recognized", defel->defname);
    }

    // Apply attribute changes
    if (volatility_item)
        procForm->provolatile = interpret_func_volatility(volatility_item);
    if (strict_item)
        procForm->proisstrict = boolVal(strict_item->arg);
    if (security_def_item)
        procForm->prosecdef = boolVal(security_def_item->arg);

    if (leakproof_item) {
        procForm->proleakproof = boolVal(leakproof_item->arg);
        if (procForm->proleakproof && !superuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                           errmsg("only superuser can define a leakproof function")));
    }

    if (cost_item) {
        procForm->procost = defGetNumeric(cost_item);
        if (procForm->procost <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("COST must be positive")));
    }

    if (rows_item) {
        procForm->prorows = defGetNumeric(rows_item);
        if (procForm->prorows <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("ROWS must be positive")));
        if (!procForm->proretset)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("ROWS is not applicable when function does not return a set")));
    }

    if (support_item) {
        Oid newsupport = interpret_func_support(support_item);

        // Update support function dependency
        if (OidIsValid(procForm->prosupport)) {
            if (changeDependencyFor(ProcedureRelationId, funcOid,
                                   ProcedureRelationId, procForm->prosupport,
                                   newsupport) != 1)
                elog(ERROR, "could not change support dependency for function %s",
                     get_func_name(funcOid));
        } else {
            ObjectAddress referenced;
            referenced.classId = ProcedureRelationId;
            referenced.objectId = newsupport;
            referenced.objectSubId = 0;
            recordDependencyOn(&address, &referenced, DEPENDENCY_NORMAL);
        }

        procForm->prosupport = newsupport;
    }

    if (parallel_item)
        procForm->proparallel = interpret_func_parallel(parallel_item);

    // Handle configuration parameter changes
    if (set_items) {
        Datum datum;
        bool isnull;
        ArrayType *a;
        Datum repl_val[Natts_pg_proc];
        bool repl_null[Natts_pg_proc];
        bool repl_repl[Natts_pg_proc];

        // Get existing proconfig setting
        datum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_proconfig, &isnull);
        a = isnull ? NULL : DatumGetArrayTypeP(datum);

        // Update configuration array
        a = update_proconfig_value(a, set_items);

        // Prepare tuple update
        memset(repl_repl, false, sizeof(repl_repl));
        repl_repl[Anum_pg_proc_proconfig - 1] = true;

        if (a == NULL) {
            repl_val[Anum_pg_proc_proconfig - 1] = (Datum) 0;
            repl_null[Anum_pg_proc_proconfig - 1] = true;
        } else {
            repl_val[Anum_pg_proc_proconfig - 1] = PointerGetDatum(a);
            repl_null[Anum_pg_proc_proconfig - 1] = false;
        }

        tup = heap_modify_tuple(tup, RelationGetDescr(rel),
                               repl_val, repl_null, repl_repl);
    }

    // Update catalog and cleanup
    CatalogTupleUpdate(rel, &tup->t_self, tup);
    InvokeObjectPostAlterHook(ProcedureRelationId, funcOid, 0);

    table_close(rel, NoLock);
    heap_freetuple(tup);

    return address;
}
```