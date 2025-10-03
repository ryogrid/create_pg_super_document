# AggregateCreate

## Location
[src/backend/catalog/pg_aggregate.c:46-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_aggregate.c#L46-L825)

## Overview
AggregateCreate is the core function responsible for creating new aggregate functions in PostgreSQL, validating their definitions, and inserting them into the system catalogs.

## Definition

```c
ObjectAddress
AggregateCreate(const char *aggName,
				Oid aggNamespace,
				bool replace,
				char aggKind,
				int numArgs,
				int numDirectArgs,
				oidvector *parameterTypes,
				Datum allParameterTypes,
				Datum parameterModes,
				Datum parameterNames,
				List *parameterDefaults,
				Oid variadicArgType,
				List *aggtransfnName,
				List *aggfinalfnName,
				List *aggcombinefnName,
				List *aggserialfnName,
				List *aggdeserialfnName,
				List *aggmtransfnName,
				List *aggminvtransfnName,
				List *aggmfinalfnName,
				bool finalfnExtraArgs,
				bool mfinalfnExtraArgs,
				char finalfnModify,
				char mfinalfnModify,
				List *aggsortopName,
				Oid aggTransType,
				int32 aggTransSpace,
				Oid aggmTransType,
				int32 aggmTransSpace,
				const char *agginitval,
				const char *aggminitval,
				char proparallel)
```
## Detailed Description
AggregateCreate is the central function for creating aggregate functions in PostgreSQL. It performs comprehensive validation of all aggregate components including transition functions, final functions, combine functions, and serialization/deserialization functions. The function handles different aggregate types (normal, ordered-set, hypothetical-set) and supports both single-phase and moving-aggregate implementations.

The function validates polymorphic types, ensures proper function signatures match expected patterns, checks permissions on all referenced types and functions, and creates entries in both pg_proc and pg_aggregate system catalogs. It also establishes dependency relationships between the aggregate and all its component functions.

## Parameters / Member Variables
- `*aggName`: Name of the aggregate function being created
- `aggNamespace`: Namespace (schema) OID where the aggregate will be created
- `replace`: Whether to replace an existing aggregate with the same signature
- `aggKind`: Type of aggregate (normal, ordered-set, or hypothetical-set)
- `numArgs`: Total number of aggregate arguments
- `numDirectArgs`: Number of direct arguments (for ordered-set aggregates)
- `*parameterTypes`: Vector of parameter type OIDs
- `allParameterTypes`: All parameter types including OUT parameters
- `parameterModes`: Parameter modes (IN, OUT, INOUT, VARIADIC)
- `parameterNames`: Parameter names
- `*parameterDefaults`: Default values for parameters
- `variadicArgType`: Type OID for variadic arguments, if any
- `*aggtransfnName`: Name of the state transition function
- `*aggfinalfnName`: Name of the final function (optional)
- `*aggcombinefnName`: Name of the combine function for parallel aggregation (optional)
- `*aggserialfnName`: Name of the serialization function (optional)
- `*aggdeserialfnName`: Name of the deserialization function (optional)
- `*aggmtransfnName`: Name of the forward transition function for moving aggregates (optional)
- `*aggminvtransfnName`: Name of the inverse transition function for moving aggregates (optional)
- `*aggmfinalfnName`: Name of the final function for moving aggregates (optional)
- `finalfnExtraArgs`: Whether final function receives extra arguments
- `mfinalfnExtraArgs`: Whether moving-aggregate final function receives extra arguments
- `finalfnModify`: Whether final function modifies transition state
- `mfinalfnModify`: Whether moving-aggregate final function modifies transition state
- `*aggsortopName`: Name of the sort operator for ordered-set aggregates (optional)
- `aggTransType`: OID of the state transition data type
- `aggTransSpace`: Estimated average size of transition state
- `aggmTransType`: OID of the moving-aggregate transition data type (optional)
- `aggmTransSpace`: Estimated average size of moving-aggregate transition state
- `*agginitval`: Initial value for transition state (optional)
- `*aggminitval`: Initial value for moving-aggregate transition state (optional)
- `proparallel`: Parallel safety level of the aggregate
## Dependencies
- Functions called/Symbols referenced:
  - [lookup_agg_function](../l/lookup_agg_function.md): Validates and finds component functions
  - [check_valid_polymorphic_signature](../c/check_valid_polymorphic_signature.md): Validates polymorphic type signatures
  - [ProcedureCreate](../P/ProcedureCreate.md): Creates the pg_proc entry for the aggregate
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md): Checks type compatibility
  - [LookupOperName](../L/LookupOperName.md): Finds sort operators for ordered-set aggregates
  - [object_aclcheck](../o/object_aclcheck.md): Checks permissions on types and functions
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md): Establishes dependency relationships

- Called from (representative examples):
  - [DefineAggregate](../D/DefineAggregate.md): Main entry point from CREATE AGGREGATE command

## Notes and Other Information
The function performs extensive validation including checking that transition function return types match declared transition types, ensuring polymorphic consistency across all components, and validating that moving-aggregate implementations produce the same result type as regular implementations. It supports parallel aggregation through combine functions and window functions through moving-aggregate implementations with forward/inverse transition functions.

The function handles replacement of existing aggregates carefully, ensuring that critical properties like aggregate kind and number of direct arguments cannot be changed, as these would break existing callers.

## Simplified Source

```c
ObjectAddress
AggregateCreate(const char *aggName, Oid aggNamespace, bool replace, char aggKind,
                int numArgs, int numDirectArgs, oidvector *parameterTypes,
                /* ... many other parameters ... */
                char proparallel)
{
    // Basic validation
    if (!aggName) elog(ERROR, "no aggregate name supplied");
    if (!aggtransfnName) elog(ERROR, "aggregate must have a transition function");
    if (numArgs < 0 || numArgs > FUNC_MAX_ARGS - 1)
        ereport(ERROR, "too many arguments");

    // Validate polymorphic types
    detailmsg = check_valid_polymorphic_signature(aggTransType, aggArgTypes, numArgs);
    if (detailmsg) ereport(ERROR, "cannot determine transition data type");

    // Special validation for ordered-set and hypothetical aggregates
    if (AGGKIND_IS_ORDERED_SET(aggKind) && OidIsValid(variadicArgType) &&
        variadicArgType != ANYOID)
        ereport(ERROR, "variadic ordered-set aggregate must use VARIADIC type ANY");

    // Build function arguments and find transition function
    if (AGGKIND_IS_ORDERED_SET(aggKind)) {
        // Set up args for ordered-set: transtype + aggregated args only
        nargs_transfn = numArgs - numDirectArgs + 1;
        fnArgs[0] = aggTransType;
        memcpy(fnArgs + 1, aggArgTypes + (numArgs - (nargs_transfn - 1)),
               (nargs_transfn - 1) * sizeof(Oid));
    } else {
        // Set up args for normal aggregate: transtype + all args
        nargs_transfn = numArgs + 1;
        fnArgs[0] = aggTransType;
        memcpy(fnArgs + 1, aggArgTypes, numArgs * sizeof(Oid));
    }

    // Lookup and validate transition function
    transfn = lookup_agg_function(aggtransfnName, nargs_transfn, fnArgs,
                                  variadicArgType, &rettype);
    if (rettype != aggTransType)
        ereport(ERROR, "return type mismatch");

    // Validate function strictness for NULL initial values
    tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(transfn));
    proc = (Form_pg_proc) GETSTRUCT(tup);
    if (proc->proisstrict && agginitval == NULL &&
        !IsBinaryCoercible(aggArgTypes[0], aggTransType))
        ereport(ERROR, "must not omit initial value");
    ReleaseSysCache(tup);

    // Handle optional functions (finalfn, combinefn, etc.)
    if (aggfinalfnName) {
        finalfn = lookup_agg_function(aggfinalfnName, nargs_finalfn,
                                      fnArgs, ffnVariadicArgType, &finaltype);
        if (finalfnExtraArgs && func_strict(finalfn))
            ereport(ERROR, "final function with extra arguments must not be strict");
    } else {
        finaltype = aggTransType;
    }

    // Handle combine function for parallel aggregation
    if (aggcombinefnName) {
        fnArgs[0] = fnArgs[1] = aggTransType;
        combinefn = lookup_agg_function(aggcombinefnName, 2, fnArgs,
                                        InvalidOid, &combineType);
        if (combineType != aggTransType)
            ereport(ERROR, "combine function return type mismatch");
    }

    // Validate serialization/deserialization functions
    if (aggserialfnName) {
        fnArgs[0] = INTERNALOID;
        serialfn = lookup_agg_function(aggserialfnName, 1, fnArgs, InvalidOid, &rettype);
        if (rettype != BYTEAOID) ereport(ERROR, "serialization function type mismatch");
    }

    // Check permissions on all types
    for (i = 0; i < numArgs; i++) {
        aclresult = object_aclcheck(TypeRelationId, aggArgTypes[i], GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK) aclcheck_error_type(aclresult, aggArgTypes[i]);
    }

    // Create the aggregate's pg_proc entry
    myself = ProcedureCreate(aggName, aggNamespace, replace, false, finaltype,
                             GetUserId(), INTERNALlanguageId, InvalidOid,
                             "aggregate_dummy", NULL, NULL, PROKIND_AGGREGATE,
                             /* ... other parameters ... */);
    procOid = myself.objectId;

    // Create pg_aggregate catalog entry
    aggdesc = table_open(AggregateRelationId, RowExclusiveLock);

    // Initialize catalog values
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    values[Anum_pg_aggregate_aggfnoid - 1] = ObjectIdGetDatum(procOid);
    values[Anum_pg_aggregate_aggtransfn - 1] = ObjectIdGetDatum(transfn);
    values[Anum_pg_aggregate_aggfinalfn - 1] = ObjectIdGetDatum(finalfn);
    // ... set other catalog values ...

    // Insert or update catalog entry
    if (replace && HeapTupleIsValid(oldtup)) {
        tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
        CatalogTupleUpdate(aggdesc, &tup->t_self, tup);
    } else {
        tup = heap_form_tuple(tupDesc, values, nulls);
        CatalogTupleInsert(aggdesc, tup);
    }

    table_close(aggdesc, RowExclusiveLock);

    // Record dependencies on component functions
    addrs = new_object_addresses();
    ObjectAddressSet(referenced, ProcedureRelationId, transfn);
    add_exact_object_address(&referenced, addrs);
    if (OidIsValid(finalfn)) {
        ObjectAddressSet(referenced, ProcedureRelationId, finalfn);
        add_exact_object_address(&referenced, addrs);
    }
    // ... add other dependencies ...
    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);

    return myself;
}
```