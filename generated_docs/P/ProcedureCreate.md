# ProcedureCreate

## Location
[src/backend/catalog/pg_proc.c:70-724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_proc.c#L70-L724)

## Overview
Creates a new function/procedure in the PostgreSQL catalog (pg_proc table) or replaces an existing one, handling all validation, dependency tracking, and ACL setup.

## Definition

```c
struct array inputs */
	if (allParameterTypes != PointerGetDatum(NULL))
	{
		/*
		 * We expect the array to be a 1-D OID array; verify that. We don't
		 * need to use deconstruct_array() since the array data is just going
		 * to look like a C array of OID values.
		 */
		ArrayType  *allParamArray = (ArrayType *) DatumGetPointer(allParameterTypes);

		allParamCount = ARR_DIMS(allParamArray)[0];
		if (ARR_NDIM(allParamArray) != 1 ||
			allParamCount <= 0 ||
			ARR_HASNULL(allParamArray) ||
			ARR_ELEMTYPE(allParamArray) != OIDOID)
			elog(ERROR, "allParameterTypes is not a 1-D Oid array");
		allParams = (Oid *) ARR_DATA_PTR(allParamArray);
		Assert(allParamCount >= parameterCount);
		/* we assume caller got the contents right */
	}
	else
	{
		allParamCount = parameterCount;
		allParams = parameterTypes->values;
	}

	if (parameterModes != PointerGetDatum(NULL))
	{
		/*
		 * We expect the array to be a 1-D CHAR array; verify that. We don't
		 * need to use deconstruct_array() since the array data is just going
		 * to look like a C array of char values.
		 */
		ArrayType  *modesArray = (ArrayType *) DatumGetPointer(parameterModes);

		if (ARR_NDIM(modesArray) != 1 ||
			ARR_DIMS(modesArray)[0] != allParamCount ||
			ARR_HASNULL(modesArray) ||
			ARR_ELEMTYPE(modesArray) != CHAROID)
			elog(ERROR, "parameterModes is not a 1-D char array");
		paramModes = (char *) ARR_DATA_PTR(modesArray);
	}

	/*
	 * Do not allow polymorphic return type unless there is a polymorphic
	 * input argument that we can use to deduce the actual return type.
	 */
	detailmsg = check_valid_polymorphic_signature(returnType,
												  parameterTypes->values,
												  parameterCount);
```
## Detailed Description
ProcedureCreate is the core function responsible for creating or updating function/procedure definitions in PostgreSQL's system catalog. It performs extensive validation of parameters, handles polymorphic and internal types, manages dependencies, validates function signatures, and maintains proper ACL permissions.

The function handles both new function creation and replacement of existing functions (when replace=true). For replacements, it enforces strict compatibility rules to prevent breaking existing callers - return types cannot change, parameter names cannot be modified, and default parameter types must remain consistent.

Key operations include:
- Parameter validation and type checking for polymorphic and internal types
- Variadic parameter handling and validation
- Dependency recording for all referenced objects (types, languages, transforms, etc.)
- Function body validation using language-specific validators
- ACL (access control list) setup with default permissions
- Statistics initialization for the new function

## Parameters / Member Variables
- `procedureName`: Name of the function/procedure to create
- `procNamespace`: OID of the namespace where the function will be created
- `replace`: Whether to replace an existing function with the same signature
- `returnsSet`: Whether the function returns a set of values
- `returnType`: OID of the function's return type
- `proowner`: OID of the function owner
- `languageObjectId`: OID of the implementation language (SQL, C, etc.)
- `languageValidator`: OID of the validator function for this language
- `prosrc`: Source code of the function
- `probin`: Binary/library path for compiled functions (NULL for SQL functions)
- `prosqlbody`: Parsed SQL body for SQL language functions
- `prokind`: Function kind ('f'=function, 'p'=procedure, 'a'=aggregate, 'w'=window)
- `security_definer`: Whether function runs with definer's privileges
- `isLeakProof`: Whether function is guaranteed not to leak information
- `isStrict`: Whether function returns NULL on any NULL input
- `volatility`: Volatility level ('i'=immutable, 's'=stable, 'v'=volatile)
- `parallel`: Parallel safety ('s'=safe, 'r'=restricted, 'u'=unsafe)
- `parameterTypes`: Array of input parameter type OIDs
- `allParameterTypes`: Array including all parameter types (IN, OUT, INOUT, VARIADIC)
- `parameterModes`: Array of parameter modes (IN, OUT, INOUT, VARIADIC)
- `parameterNames`: Array of parameter names
- `parameterDefaults`: List of default value expressions for parameters
- `trftypes`: Array of transform types for this function
- `proconfig`: Configuration parameters for this function
- `prosupport`: OID of support function for this function
- `procost`: Estimated execution cost
- `prorows`: Estimated number of result rows (for set-returning functions)

## Dependencies
- Functions called/Symbols referenced:
  - [check_valid_polymorphic_signature](../c/check_valid_polymorphic_signature.md): Validates polymorphic type usage
  - [check_valid_internal_signature](../c/check_valid_internal_signature.md): Validates internal type usage
  - [SearchSysCache3](../S/SearchSysCache3.md): Searches for existing function definition
  - [object_ownercheck](../o/object_ownercheck.md): Verifies ownership permissions
  - [build_function_result_tupdesc_t](../b/build_function_result_tupdesc_t.md): Builds tuple descriptor for RECORD return types
  - [get_user_default_acl](../g/get_user_default_acl.md): Gets default ACL for the function
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md): Records all object dependencies
  - OidFunctionCall1: Calls language validator function
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Makes new tuple visible to validator

- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md): Main entry point for CREATE FUNCTION command
  - [AggregateCreate](../A/AggregateCreate.md): Creates the final function for aggregate definitions
  - [makeRangeConstructors](../m/makeRangeConstructors.md): Creates constructor functions for range types
  - [makeMultirangeConstructors](../m/makeMultirangeConstructors.md): Creates constructor functions for multirange types

## Notes and Other Information
- The function enforces strict backward compatibility when replacing existing functions to prevent breaking dependent objects like views and rules
- Polymorphic type validation ensures that polymorphic return types have corresponding polymorphic input parameters for type resolution
- Internal type usage is restricted to prevent unsafe operations with pseudo-types
- Function validation is performed using language-specific validator functions, but only when check_function_bodies GUC is enabled
- Variadic parameters must be the last input parameter and are validated for proper array type usage
- The function creates comprehensive dependency records to track all objects the function depends on
- Statistics are initialized for new functions to support query planning cost estimation

## Simplified Source

```c
ObjectAddress ProcedureCreate(const char *procedureName,
                             Oid procNamespace,
                             bool replace,
                             bool returnsSet,
                             Oid returnType,
                             Oid proowner,
                             Oid languageObjectId,
                             Oid languageValidator,
                             const char *prosrc,
                             const char *probin,
                             Node *prosqlbody,
                             char prokind,
                             bool security_definer,
                             bool isLeakProof,
                             bool isStrict,
                             char volatility,
                             char parallel,
                             oidvector *parameterTypes,
                             Datum allParameterTypes,
                             Datum parameterModes,
                             Datum parameterNames,
                             List *parameterDefaults,
                             Datum trftypes,
                             Datum proconfig,
                             Oid prosupport,
                             float4 procost,
                             float4 prorows)
{
    Oid retval;
    int parameterCount = parameterTypes->dim1;
    int allParamCount;
    Oid *allParams;
    char *paramModes = NULL;
    Oid variadicType = InvalidOid;
    Relation rel;
    HeapTuple tup, oldtup;
    bool nulls[Natts_pg_proc];
    Datum values[Natts_pg_proc];
    bool replaces[Natts_pg_proc];
    bool is_update;
    ObjectAddress myself, referenced;

    // Validate parameter count
    if (parameterCount < 0 || parameterCount > FUNC_MAX_ARGS) {
        ereport(ERROR, "functions cannot have more than %d arguments", FUNC_MAX_ARGS);
    }

    // Process parameter arrays
    if (allParameterTypes != PointerGetDatum(NULL)) {
        ArrayType *allParamArray = (ArrayType *) DatumGetPointer(allParameterTypes);
        allParamCount = ARR_DIMS(allParamArray)[0];
        // Validate array format
        if (ARR_NDIM(allParamArray) != 1 || allParamCount <= 0 ||
            ARR_HASNULL(allParamArray) || ARR_ELEMTYPE(allParamArray) != OIDOID) {
            elog(ERROR, "allParameterTypes is not a 1-D Oid array");
        }
        allParams = (Oid *) ARR_DATA_PTR(allParamArray);
    } else {
        allParamCount = parameterCount;
        allParams = parameterTypes->values;
    }

    // Process parameter modes array
    if (parameterModes != PointerGetDatum(NULL)) {
        ArrayType *modesArray = (ArrayType *) DatumGetPointer(parameterModes);
        // Validate modes array format
        if (ARR_NDIM(modesArray) != 1 ||
            ARR_DIMS(modesArray)[0] != allParamCount ||
            ARR_HASNULL(modesArray) || ARR_ELEMTYPE(modesArray) != CHAROID) {
            elog(ERROR, "parameterModes is not a 1-D char array");
        }
        paramModes = (char *) ARR_DATA_PTR(modesArray);
    }

    // Validate polymorphic types
    char *detailmsg = check_valid_polymorphic_signature(returnType,
                                                       parameterTypes->values,
                                                       parameterCount);
    if (detailmsg) {
        ereport(ERROR, "cannot determine result data type");
    }

    // Validate internal types
    detailmsg = check_valid_internal_signature(returnType,
                                              parameterTypes->values,
                                              parameterCount);
    if (detailmsg) {
        ereport(ERROR, "unsafe use of pseudo-type \"internal\"");
    }

    // Validate OUT parameter types
    if (allParameterTypes != PointerGetDatum(NULL)) {
        for (int i = 0; i < allParamCount; i++) {
            if (paramModes == NULL ||
                paramModes[i] == PROARGMODE_IN ||
                paramModes[i] == PROARGMODE_VARIADIC) {
                continue; // Skip input-only params
            }

            // Validate polymorphic and internal types for OUT params
            detailmsg = check_valid_polymorphic_signature(allParams[i],
                                                         parameterTypes->values,
                                                         parameterCount);
            if (detailmsg) {
                ereport(ERROR, "cannot determine result data type");
            }

            detailmsg = check_valid_internal_signature(allParams[i],
                                                      parameterTypes->values,
                                                      parameterCount);
            if (detailmsg) {
                ereport(ERROR, "unsafe use of pseudo-type \"internal\"");
            }
        }
    }

    // Identify variadic parameter type
    if (paramModes != NULL) {
        for (int i = 0; i < allParamCount; i++) {
            if (paramModes[i] == PROARGMODE_VARIADIC) {
                if (OidIsValid(variadicType)) {
                    elog(ERROR, "variadic parameter must be last");
                }

                // Handle special variadic types
                switch (allParams[i]) {
                    case ANYOID:
                        variadicType = ANYOID;
                        break;
                    case ANYARRAYOID:
                        variadicType = ANYELEMENTOID;
                        break;
                    case ANYCOMPATIBLEARRAYOID:
                        variadicType = ANYCOMPATIBLEOID;
                        break;
                    default:
                        variadicType = get_element_type(allParams[i]);
                        if (!OidIsValid(variadicType)) {
                            elog(ERROR, "variadic parameter is not an array");
                        }
                        break;
                }
            }
        }
    }

    // Initialize tuple values
    memset(nulls, false, sizeof(nulls));
    memset(values, 0, sizeof(values));
    memset(replaces, true, sizeof(replaces));

    // Fill in basic procedure attributes
    NameData procname;
    namestrcpy(&procname, procedureName);
    values[Anum_pg_proc_proname - 1] = NameGetDatum(&procname);
    values[Anum_pg_proc_pronamespace - 1] = ObjectIdGetDatum(procNamespace);
    values[Anum_pg_proc_proowner - 1] = ObjectIdGetDatum(proowner);
    values[Anum_pg_proc_prolang - 1] = ObjectIdGetDatum(languageObjectId);
    values[Anum_pg_proc_procost - 1] = Float4GetDatum(procost);
    values[Anum_pg_proc_prorows - 1] = Float4GetDatum(prorows);
    values[Anum_pg_proc_provariadic - 1] = ObjectIdGetDatum(variadicType);
    values[Anum_pg_proc_prosupport - 1] = ObjectIdGetDatum(prosupport);
    values[Anum_pg_proc_prokind - 1] = CharGetDatum(prokind);
    values[Anum_pg_proc_prosecdef - 1] = BoolGetDatum(security_definer);
    values[Anum_pg_proc_proleakproof - 1] = BoolGetDatum(isLeakProof);
    values[Anum_pg_proc_proisstrict - 1] = BoolGetDatum(isStrict);
    values[Anum_pg_proc_proretset - 1] = BoolGetDatum(returnsSet);
    values[Anum_pg_proc_provolatile - 1] = CharGetDatum(volatility);
    values[Anum_pg_proc_proparallel - 1] = CharGetDatum(parallel);
    values[Anum_pg_proc_pronargs - 1] = UInt16GetDatum(parameterCount);
    values[Anum_pg_proc_pronargdefaults - 1] = UInt16GetDatum(list_length(parameterDefaults));
    values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(returnType);
    values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(parameterTypes);

    // Set optional arrays (or null if not provided)
    if (allParameterTypes != PointerGetDatum(NULL))
        values[Anum_pg_proc_proallargtypes - 1] = allParameterTypes;
    else
        nulls[Anum_pg_proc_proallargtypes - 1] = true;

    if (parameterModes != PointerGetDatum(NULL))
        values[Anum_pg_proc_proargmodes - 1] = parameterModes;
    else
        nulls[Anum_pg_proc_proargmodes - 1] = true;

    if (parameterNames != PointerGetDatum(NULL))
        values[Anum_pg_proc_proargnames - 1] = parameterNames;
    else
        nulls[Anum_pg_proc_proargnames - 1] = true;

    if (parameterDefaults != NIL)
        values[Anum_pg_proc_proargdefaults - 1] = CStringGetTextDatum(nodeToString(parameterDefaults));
    else
        nulls[Anum_pg_proc_proargdefaults - 1] = true;

    if (trftypes != PointerGetDatum(NULL))
        values[Anum_pg_proc_protrftypes - 1] = trftypes;
    else
        nulls[Anum_pg_proc_protrftypes - 1] = true;

    values[Anum_pg_proc_prosrc - 1] = CStringGetTextDatum(prosrc);

    if (probin)
        values[Anum_pg_proc_probin - 1] = CStringGetTextDatum(probin);
    else
        nulls[Anum_pg_proc_probin - 1] = true;

    if (prosqlbody)
        values[Anum_pg_proc_prosqlbody - 1] = CStringGetTextDatum(nodeToString(prosqlbody));
    else
        nulls[Anum_pg_proc_prosqlbody - 1] = true;

    if (proconfig != PointerGetDatum(NULL))
        values[Anum_pg_proc_proconfig - 1] = proconfig;
    else
        nulls[Anum_pg_proc_proconfig - 1] = true;

    // Open catalog and check for existing function
    rel = table_open(ProcedureRelationId, RowExclusiveLock);
    TupleDesc tupDesc = RelationGetDescr(rel);

    oldtup = SearchSysCache3(PROCNAMEARGSNSP,
                            PointerGetDatum(procedureName),
                            PointerGetDatum(parameterTypes),
                            ObjectIdGetDatum(procNamespace));

    if (HeapTupleIsValid(oldtup)) {
        // Handle function replacement
        Form_pg_proc oldproc = (Form_pg_proc) GETSTRUCT(oldtup);

        if (!replace) {
            ereport(ERROR, "function already exists with same argument types");
        }

        if (!object_ownercheck(ProcedureRelationId, oldproc->oid, proowner)) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION, procedureName);
        }

        // Validate function kind compatibility
        if (oldproc->prokind != prokind) {
            ereport(ERROR, "cannot change routine kind");
        }

        // Validate return type compatibility
        if (returnType != oldproc->prorettype || returnsSet != oldproc->proretset) {
            ereport(ERROR, prokind == PROKIND_PROCEDURE ?
                   "cannot change whether a procedure has output parameters" :
                   "cannot change return type of existing function");
        }

        // Additional compatibility checks for RECORD types and parameter names omitted for brevity

        // Preserve original OID, ownership, and ACL
        replaces[Anum_pg_proc_oid - 1] = false;
        replaces[Anum_pg_proc_proowner - 1] = false;
        replaces[Anum_pg_proc_proacl - 1] = false;

        tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
        CatalogTupleUpdate(rel, &tup->t_self, tup);
        ReleaseSysCache(oldtup);
        is_update = true;
    } else {
        // Create new function
        Acl *proacl = get_user_default_acl(OBJECT_FUNCTION, proowner, procNamespace);
        if (proacl != NULL)
            values[Anum_pg_proc_proacl - 1] = PointerGetDatum(proacl);
        else
            nulls[Anum_pg_proc_proacl - 1] = true;

        Oid newOid = GetNewOidWithIndex(rel, ProcedureOidIndexId, Anum_pg_proc_oid);
        values[Anum_pg_proc_oid - 1] = ObjectIdGetDatum(newOid);
        tup = heap_form_tuple(tupDesc, values, nulls);
        CatalogTupleInsert(rel, tup);
        is_update = false;
    }

    retval = ((Form_pg_proc) GETSTRUCT(tup))->oid;

    // Create dependencies
    if (is_update) {
        deleteDependencyRecordsFor(ProcedureRelationId, retval, true);
    }

    ObjectAddresses *addrs = new_object_addresses();
    ObjectAddressSet(myself, ProcedureRelationId, retval);

    // Record dependencies on namespace, language, return type, parameter types, etc.
    ObjectAddressSet(referenced, NamespaceRelationId, procNamespace);
    add_exact_object_address(&referenced, addrs);

    ObjectAddressSet(referenced, LanguageRelationId, languageObjectId);
    add_exact_object_address(&referenced, addrs);

    ObjectAddressSet(referenced, TypeRelationId, returnType);
    add_exact_object_address(&referenced, addrs);

    // Dependencies on parameter types
    for (int i = 0; i < allParamCount; i++) {
        ObjectAddressSet(referenced, TypeRelationId, allParams[i]);
        add_exact_object_address(&referenced, addrs);
    }

    // Support function dependency
    if (OidIsValid(prosupport)) {
        ObjectAddressSet(referenced, ProcedureRelationId, prosupport);
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    // Record dependencies on SQL body and parameter defaults
    if (languageObjectId == SQLlanguageId && prosqlbody) {
        recordDependencyOnExpr(&myself, prosqlbody, NIL, DEPENDENCY_NORMAL);
    }

    if (parameterDefaults) {
        recordDependencyOnExpr(&myself, (Node *) parameterDefaults, NIL, DEPENDENCY_NORMAL);
    }

    // Record ownership and ACL dependencies for new functions
    if (!is_update) {
        recordDependencyOnOwner(ProcedureRelationId, retval, proowner);
        recordDependencyOnNewAcl(ProcedureRelationId, retval, 0, proowner, proacl);
    }

    recordDependencyOnCurrentExtension(&myself, is_update);

    heap_freetuple(tup);
    InvokeObjectPostCreateHook(ProcedureRelationId, retval, 0);
    table_close(rel, RowExclusiveLock);

    // Validate function body if validator exists
    if (OidIsValid(languageValidator)) {
        CommandCounterIncrement();

        ArrayType *set_items = NULL;
        int save_nestlevel = 0;

        // Apply function configuration settings during validation
        if (check_function_bodies) {
            set_items = (ArrayType *) DatumGetPointer(proconfig);
            if (set_items) {
                save_nestlevel = NewGUCNestLevel();
                ProcessGUCArray(set_items,
                               (superuser() ? PGC_SUSET : PGC_USERSET),
                               PGC_S_SESSION,
                               GUC_ACTION_SAVE);
            }
        }

        OidFunctionCall1(languageValidator, ObjectIdGetDatum(retval));

        if (set_items) {
            AtEOXact_GUC(true, save_nestlevel);
        }
    }

    // Initialize function statistics
    if (!is_update) {
        pgstat_create_function(retval);
    }

    return myself;
}
```