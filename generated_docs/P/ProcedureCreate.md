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