# CreateFunction

## Location
[src/backend/commands/functioncmds.c:1011-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1011-L1292)

## Overview
Executes a CREATE FUNCTION or CREATE PROCEDURE utility statement, orchestrating the complete process of function definition validation, parsing, and catalog registration.

## Definition
```c
ObjectAddress CreateFunction(ParseState *pstate, CreateFunctionStmt *stmt)
```

## Detailed Description
This function is the main entry point for processing CREATE FUNCTION and CREATE PROCEDURE statements. It performs comprehensive validation of all function attributes, handles language-specific processing, validates permissions and security constraints, processes parameter lists and return types, interprets the function body according to the target language, and finally creates the function in the system catalog.

The function handles both regular functions and procedures, with special logic for different programming languages (C, SQL, PL/pgSQL, etc.). It validates user privileges for the target namespace and language, processes transform types, handles default values for cost and row estimates, and coordinates with ProcedureCreate() for the actual catalog entry creation.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting and source text tracking
- `stmt`: CreateFunctionStmt containing all parsed function definition elements

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md) (resolves function name and namespace)
  - [object_aclcheck](../o/object_aclcheck.md), aclcheck_error (permission checking)
  - [compute_function_attributes](../c/compute_function_attributes.md) (processes function options and attributes)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (language catalog lookups)
  - [extension_file_exists](../e/extension_file_exists.md) (checks for language extensions)
  - [superuser](../s/superuser.md) (privilege validation)
  - [typenameTypeId](../t/typenameTypeId.md), get_base_element_type (type resolution)
  - [get_transform_oid](../g/get_transform_oid.md) (transform function validation)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md) (parameter processing)
  - [compute_return_type](../c/compute_return_type.md) (return type resolution)
  - [construct_array_builtin](../c/construct_array_builtin.md) (array construction for transforms)
  - [interpret_AS_clause](../i/interpret_AS_clause.md) (function body processing)
  - [ProcedureCreate](../P/ProcedureCreate.md) (final catalog entry creation)
  - Various constants: PROVOLATILE_VOLATILE, PROPARALLEL_UNSAFE, PROKIND_*
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1655)

## Notes and Other Information
- Handles both CREATE FUNCTION and CREATE PROCEDURE with shared logic and procedure-specific branches
- Validates language permissions: trusted languages require USAGE, untrusted languages require superuser
- Only superusers can create leakproof functions due to security implications
- Sets intelligent defaults for COST (1 for C/internal, 100 for others) and ROWS (1000 for set-returning, 0 otherwise)
- Supports transform types for custom type handling in procedural languages
- Validates that ROWS parameter is only specified for set-returning functions
- Coordinates with the parser state to provide accurate error locations and context
- Returns ObjectAddress for dependency tracking and object management
- Central orchestrator in PostgreSQL's function DDL implementation, calling multiple specialized helper functions

## Simplified Source

```c
ObjectAddress
CreateFunction(ParseState *pstate, CreateFunctionStmt *stmt)
{
    char *funcname;
    Oid namespaceId, languageOid, languageValidator;
    char *language = NULL;
    oidvector *parameterTypes;
    List *parameterTypes_list = NIL;
    ArrayType *allParameterTypes, *parameterModes, *parameterNames;
    List *inParameterNames_list = NIL;
    List *parameterDefaults;
    Oid variadicArgType, requiredResultType;
    Oid prorettype;
    bool returnsSet, isWindowFunc, isStrict, security, isLeakProof;
    char volatility, parallel;
    ArrayType *proconfig;
    float4 procost = -1, prorows = -1;
    Oid prosupport = InvalidOid;
    List *as_clause;
    char *prosrc_str, *probin_str;
    Node *prosqlbody;

    // Get namespace and check creation privileges
    namespaceId = QualifiedNameGetCreationNamespace(stmt->funcname, &funcname);

    AclResult aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceId));
    }

    // Set default attributes
    isWindowFunc = false;
    isStrict = false;
    security = false;
    isLeakProof = false;
    volatility = PROVOLATILE_VOLATILE;
    proconfig = NULL;
    parallel = PROPARALLEL_UNSAFE;

    // Parse function attributes from options
    compute_function_attributes(pstate, stmt->is_procedure, stmt->options,
                               &as_clause, &language, NULL,
                               &isWindowFunc, &volatility, &isStrict,
                               &security, &isLeakProof, &proconfig,
                               &procost, &prorows, &prosupport, &parallel);

    // Default language to SQL if SQL body provided
    if (!language) {
        if (stmt->sql_body)
            language = "sql";
        else
            ereport(ERROR, "no language specified");
    }

    // Validate language exists and check permissions
    HeapTuple languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
    if (!HeapTupleIsValid(languageTuple)) {
        ereport(ERROR, "language does not exist");
    }

    Form_pg_language languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);
    languageOid = languageStruct->oid;

    // Check language permissions
    if (languageStruct->lanpltrusted) {
        // Trusted language - need USAGE privilege
        aclresult = object_aclcheck(LanguageRelationId, languageOid, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK) {
            aclcheck_error(aclresult, OBJECT_LANGUAGE, NameStr(languageStruct->lanname));
        }
    } else {
        // Untrusted language - must be superuser
        if (!superuser()) {
            aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_LANGUAGE, NameStr(languageStruct->lanname));
        }
    }

    languageValidator = languageStruct->lanvalidator;
    ReleaseSysCache(languageTuple);

    // Only superuser can create leakproof functions
    if (isLeakProof && !superuser()) {
        ereport(ERROR, "only superuser can define a leakproof function");
    }

    // Process parameter list
    interpret_function_parameter_list(pstate, stmt->parameters, languageOid,
                                    stmt->is_procedure ? OBJECT_PROCEDURE : OBJECT_FUNCTION,
                                    &parameterTypes, &parameterTypes_list,
                                    &allParameterTypes, &parameterModes, &parameterNames,
                                    &inParameterNames_list, &parameterDefaults,
                                    &variadicArgType, &requiredResultType);

    // Determine return type
    if (stmt->is_procedure) {
        prorettype = requiredResultType ? requiredResultType : VOIDOID;
        returnsSet = false;
    } else if (stmt->returnType) {
        compute_return_type(stmt->returnType, languageOid, &prorettype, &returnsSet);
        if (OidIsValid(requiredResultType) && prorettype != requiredResultType) {
            ereport(ERROR, "function result type conflicts with OUT parameters");
        }
    } else if (OidIsValid(requiredResultType)) {
        prorettype = requiredResultType;
        returnsSet = false;
    } else {
        ereport(ERROR, "function result type must be specified");
    }

    // Process function body (AS clause or SQL body)
    interpret_AS_clause(languageOid, language, funcname, as_clause, stmt->sql_body,
                       parameterTypes_list, inParameterNames_list,
                       &prosrc_str, &probin_str, &prosqlbody,
                       pstate->p_sourcetext);

    // Set default cost and rows if not specified
    if (procost < 0) {
        if (languageOid == INTERNALlanguageId || languageOid == ClanguageId)
            procost = 1;
        else
            procost = 100;
    }

    if (prorows < 0) {
        if (returnsSet)
            prorows = 1000;
        else
            prorows = 0;
    } else if (!returnsSet) {
        ereport(ERROR, "ROWS not applicable when function does not return a set");
    }

    // Create the function in the catalog
    return ProcedureCreate(funcname, namespaceId, stmt->replace, returnsSet,
                          prorettype, GetUserId(), languageOid, languageValidator,
                          prosrc_str, probin_str, prosqlbody,
                          stmt->is_procedure ? PROKIND_PROCEDURE :
                          (isWindowFunc ? PROKIND_WINDOW : PROKIND_FUNCTION),
                          security, isLeakProof, isStrict, volatility, parallel,
                          parameterTypes, PointerGetDatum(allParameterTypes),
                          PointerGetDatum(parameterModes), PointerGetDatum(parameterNames),
                          parameterDefaults, PointerGetDatum(NULL),
                          PointerGetDatum(proconfig), prosupport, procost, prorows);
}
```