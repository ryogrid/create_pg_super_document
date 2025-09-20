# DefineAggregate

## Location
[src/backend/commands/aggregatecmds.c:53-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/aggregatecmds.c#L53-L477)

## Overview
DefineAggregate is the top-level function responsible for parsing and processing the CREATE AGGREGATE command in PostgreSQL, handling all aspects of aggregate function definition validation and creation.

## Definition

```c
struct the output of the aggr_args grammar production */
	if (!oldstyle)
	{
		Assert(list_length(args) == 2);
		numDirectArgs = intVal(lsecond(args));
		if (numDirectArgs >= 0)
			aggKind = AGGKIND_ORDERED_SET;
		else
			numDirectArgs = 0;
		args = linitial_node(List, args);
	}

	/* Examine aggregate's definition clauses */
	foreach(pl, parameters)
	{
		DefElem    *defel = lfirst_node(DefElem, pl);

		/*
		 * sfunc1, stype1, and initcond1 are accepted as obsolete spellings
		 * for sfunc, stype, initcond.
		 */
		if (strcmp(defel->defname, "sfunc") == 0)
			transfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "sfunc1") == 0)
			transfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "finalfunc") == 0)
			finalfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "combinefunc") == 0)
			combinefuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "serialfunc") == 0)
			serialfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "deserialfunc") == 0)
			deserialfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "msfunc") == 0)
			mtransfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "minvfunc") == 0)
			minvtransfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "mfinalfunc") == 0)
			mfinalfuncName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "finalfunc_extra") == 0)
			finalfuncExtraArgs = defGetBoolean(defel);
		else if (strcmp(defel->defname, "mfinalfunc_extra") == 0)
			mfinalfuncExtraArgs = defGetBoolean(defel);
		else if (strcmp(defel->defname, "finalfunc_modify") == 0)
			finalfuncModify = extractModify(defel);
		else if (strcmp(defel->defname, "mfinalfunc_modify") == 0)
			mfinalfuncModify = extractModify(defel);
		else if (strcmp(defel->defname, "sortop") == 0)
			sortoperatorName = defGetQualifiedName(defel);
		else if (strcmp(defel->defname, "basetype") == 0)
			baseType = defGetTypeName(defel);
		else if (strcmp(defel->defname, "hypothetical") == 0)
		{
			if (defGetBoolean(defel))
			{
				if (aggKind == AGGKIND_NORMAL)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
							 errmsg("only ordered-set aggregates can be hypothetical")));
				aggKind = AGGKIND_HYPOTHETICAL;
			}
		}
		else if (strcmp(defel->defname, "stype") == 0)
			transType = defGetTypeName(defel);
		else if (strcmp(defel->defname, "stype1") == 0)
			transType = defGetTypeName(defel);
		else if (strcmp(defel->defname, "sspace") == 0)
			transSpace = defGetInt32(defel);
		else if (strcmp(defel->defname, "mstype") == 0)
			mtransType = defGetTypeName(defel);
		else if (strcmp(defel->defname, "msspace") == 0)
			mtransSpace = defGetInt32(defel);
		else if (strcmp(defel->defname, "initcond") == 0)
			initval = defGetString(defel);
		else if (strcmp(defel->defname, "initcond1") == 0)
			initval = defGetString(defel);
		else if (strcmp(defel->defname, "minitcond") == 0)
			minitval = defGetString(defel);
		else if (strcmp(defel->defname, "parallel") == 0)
			parallel = defGetString(defel);
		else
			ereport(WARNING,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("aggregate attribute \"%s\" not recognized",
							defel->defname)));
	}

	/*
	 * make sure we have our required definitions
	 */
	if (transType == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("aggregate stype must be specified")));
```
## Detailed Description
DefineAggregate serves as the main entry point for CREATE AGGREGATE statement processing. It parses the aggregate definition from SQL command parameters, validates the aggregate specification including all required and optional functions (transition, final, combine, serial/deserial, moving-window variants), handles both old-style (pre-8.2) and new-style parameter formats, and performs extensive validation of aggregate parameters including type checking, permission verification, and consistency validation. The function supports various aggregate types including normal aggregates, ordered-set aggregates, and hypothetical-set aggregates, as well as moving-window aggregates with forward and inverse transition functions. After thorough validation, it delegates the actual aggregate creation to AggregateCreate.

## Parameters / Member Variables
- : Parse state containing parsing context and error information
- : Qualified name list specifying the aggregate name and optional schema
- : Function parameter list defining aggregate arguments (format depends on oldstyle flag)
- : Boolean indicating old-style syntax (pre-8.2) using BASETYPE parameter
- : List of DefElem nodes representing aggregate definition clauses (sfunc, stype, finalfunc, etc.)
- : Boolean indicating whether to replace existing aggregate (CREATE OR REPLACE)

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [extractModify](../e/extractModify.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md)
  - [AggregateCreate](../A/AggregateCreate.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
DefineAggregate handles complex validation logic for different aggregate types and parameter combinations. It supports both traditional aggregates and advanced features like moving-window aggregates, parallel execution modes, and serialization functions for custom aggregate state. The function enforces strict consistency rules between related parameters (e.g., moving-aggregate functions must be specified together) and provides detailed error messages for invalid configurations. The oldstyle parameter maintains backward compatibility with PostgreSQL versions prior to 8.2.