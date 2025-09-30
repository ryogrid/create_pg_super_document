# interpret_function_parameter_list

## Location
[src/backend/commands/functioncmds.c:183-499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L183-L499)

## Overview
Interprets and validates the function parameter list of CREATE FUNCTION, CREATE PROCEDURE, or CREATE AGGREGATE statements, extracting type information, parameter modes, names, and default values while enforcing various constraints.

## Definition

```c
struct the proper outputs as needed */
	*parameterTypes = buildoidvector(inTypes, inCount);
```
## Detailed Description
This comprehensive function processes the parameter list for database objects (functions, procedures, aggregates) by validating parameter types, modes, names, and default values. It enforces language-specific restrictions, such as preventing SQL functions from using shell types and disallowing set arguments for all object types. The function handles variadic parameters with proper validation, ensures unique parameter names within appropriate scopes, and manages parameter ordering constraints. It also processes default expressions and validates that parameters with defaults appear at the end of the input parameter list.

## Parameters / Member Variables
- : ParseState for expression transformation and validation
- : List of FunctionParameter structs representing the parameter specification
- : OID of the function language (InvalidOid for aggregates)
- : Type of object being created (OBJECT_FUNCTION, OBJECT_PROCEDURE, or OBJECT_AGGREGATE)
- : Output oidvector containing input parameter type OIDs
- : Output list of input parameter type OIDs (optional)
- : Output array of all parameter types including OUT parameters (optional)
- : Output array of parameter modes (IN, OUT, INOUT, VARIADIC, TABLE) (optional)
- : Output array of parameter names (optional)
- : Output list of input parameter names (optional)
- : Output list of default value expressions (optional)
- : Output OID of variadic array type, or InvalidOid if none
- : Output OID of required result type based on OUT parameters

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeName](../L/LookupTypeName.md): Resolves type names to type information
  - [TypeNameToString](../T/TypeNameToString.md): Converts TypeName to string representation
  - [typeTypeId](../t/typeTypeId.md): Extracts OID from type tuple
  - [object_aclcheck](../o/object_aclcheck.md): Verifies type usage permissions
  - [aclcheck_error_type](../a/aclcheck_error_type.md): Reports type access permission errors
  - [get_element_type](../g/get_element_type.md): Validates variadic array types
  - [transformExpr](../t/transformExpr.md): Processes default value expressions
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md): Type coercion for default values
  - [assign_expr_collations](../a/assign_expr_collations.md): Assigns collations to expressions
  - [contain_var_clause](../c/contain_var_clause.md): Checks for table references in defaults
  - [buildoidvector](../b/buildoidvector.md): Creates oidvector for input types
  - [construct_array_builtin](../c/construct_array_builtin.md): Creates system arrays for metadata
- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md): Function creation process
  - [DefineAggregate](../D/DefineAggregate.md): Aggregate creation process

## Notes and Other Information
- SQL functions and aggregates cannot accept shell types, but C functions can with warnings
- Set arguments (SETOF types) are prohibited for all object types  
- VARIADIC parameters must be the last input parameter and must be array types
- Parameter names must be unique within input or output parameter groups
- Default values are only allowed for input parameters and must appear at the end
- Procedures with OUT parameters always return RECORD type
- Functions with multiple OUT parameters return RECORD type
- The function enforces strict parameter ordering: regular inputs, then VARIADIC, then outputs
- Default expressions are validated to prevent table references, subqueries, and aggregates

## Simplified Source

```c
void interpret_function_parameter_list(ParseState *pstate,
                                     List *parameters,
                                     Oid languageOid,
                                     ObjectType objtype,
                                     oidvector **parameterTypes,
                                     List **parameterTypes_list,
                                     ArrayType **allParameterTypes,
                                     ArrayType **parameterModes,
                                     ArrayType **parameterNames,
                                     List **inParameterNames_list,
                                     List **parameterDefaults,
                                     Oid *variadicArgType,
                                     Oid *requiredResultType)
{
    int parameterCount = list_length(parameters);
    Oid *inTypes = palloc(parameterCount * sizeof(Oid));
    int inCount = 0, outCount = 0, varCount = 0;
    bool have_names = false, have_defaults = false;

    // Initialize output parameters
    *variadicArgType = InvalidOid;
    *requiredResultType = InvalidOid;
    *parameterDefaults = NIL;

    // Allocate work arrays
    Datum *allTypes = palloc(parameterCount * sizeof(Datum));
    Datum *paramModes = palloc(parameterCount * sizeof(Datum));
    Datum *paramNames = palloc0(parameterCount * sizeof(Datum));

    // Process each parameter
    int i = 0;
    foreach(cell, parameters) {
        FunctionParameter *fp = lfirst(cell);
        TypeName *t = fp->argType;
        FunctionParameterMode fpmode = fp->mode ?: FUNC_PARAM_IN;

        // Lookup parameter type
        Type typtup = LookupTypeName(NULL, t, NULL, false);
        if (!typtup) {
            ereport(ERROR, "type does not exist");
        }

        // Validate type - reject shell types for SQL/aggregates
        if (!type_is_defined(typtup)) {
            if (languageOid == SQLlanguageId || objtype == OBJECT_AGGREGATE) {
                ereport(ERROR, "cannot accept shell type");
            }
        }

        Oid toid = typeTypeId(typtup);
        ReleaseSysCache(typtup);

        // Check permissions and reject SETOF types
        object_aclcheck(TypeRelationId, toid, GetUserId(), ACL_USAGE);
        if (t->setof) {
            ereport(ERROR, "cannot accept set arguments");
        }

        // Handle input parameters
        if (fpmode != FUNC_PARAM_OUT && fpmode != FUNC_PARAM_TABLE) {
            if (varCount > 0) {
                ereport(ERROR, "VARIADIC parameter must be last input parameter");
            }
            inTypes[inCount++] = toid;
            if (parameterTypes_list) {
                *parameterTypes_list = lappend_oid(*parameterTypes_list, toid);
            }
        }

        // Handle output parameters
        if (fpmode != FUNC_PARAM_IN && fpmode != FUNC_PARAM_VARIADIC) {
            if (objtype == OBJECT_PROCEDURE) {
                if (varCount > 0) {
                    ereport(ERROR, "VARIADIC must be last parameter for procedures");
                }
                *requiredResultType = RECORDOID;
            } else if (outCount == 0) {
                *requiredResultType = toid;
            }
            outCount++;
        }

        // Handle VARIADIC parameters
        if (fpmode == FUNC_PARAM_VARIADIC) {
            *variadicArgType = toid;
            varCount++;

            // Validate variadic type is array-like
            if (toid != ANYARRAYOID && toid != ANYCOMPATIBLEARRAYOID &&
                toid != ANYOID && !OidIsValid(get_element_type(toid))) {
                ereport(ERROR, "VARIADIC parameter must be an array");
            }
        }

        allTypes[i] = ObjectIdGetDatum(toid);
        paramModes[i] = CharGetDatum(fpmode);

        // Handle parameter names (check for duplicates)
        if (fp->name && fp->name[0]) {
            // Simplified duplicate check logic
            check_parameter_name_conflicts(parameters, fp, fpmode);
            paramNames[i] = CStringGetTextDatum(fp->name);
            have_names = true;
        }

        // Handle default values
        if (fp->defexpr) {
            if (fpmode == FUNC_PARAM_OUT || fpmode == FUNC_PARAM_TABLE) {
                ereport(ERROR, "only input parameters can have defaults");
            }

            Node *def = transformExpr(pstate, fp->defexpr, EXPR_KIND_FUNCTION_DEFAULT);
            def = coerce_to_specific_type(pstate, def, toid, "DEFAULT");
            assign_expr_collations(pstate, def);

            // Validate no table references
            if (contain_var_clause(def)) {
                ereport(ERROR, "cannot use table references in parameter defaults");
            }

            *parameterDefaults = lappend(*parameterDefaults, def);
            have_defaults = true;
        } else if (have_defaults && fpmode != FUNC_PARAM_OUT) {
            ereport(ERROR, "parameters after defaults must also have defaults");
        }

        i++;
    }

    // Build output arrays
    *parameterTypes = buildoidvector(inTypes, inCount);

    if (outCount > 0 || varCount > 0) {
        *allParameterTypes = construct_array_builtin(allTypes, parameterCount, OIDOID);
        *parameterModes = construct_array_builtin(paramModes, parameterCount, CHAROID);
        if (outCount > 1) {
            *requiredResultType = RECORDOID;
        }
    } else {
        *allParameterTypes = NULL;
        *parameterModes = NULL;
    }

    if (have_names) {
        // Fill empty names
        for (i = 0; i < parameterCount; i++) {
            if (paramNames[i] == PointerGetDatum(NULL)) {
                paramNames[i] = CStringGetTextDatum("");
            }
        }
        *parameterNames = construct_array_builtin(paramNames, parameterCount, TEXTOID);
    } else {
        *parameterNames = NULL;
    }
}
```