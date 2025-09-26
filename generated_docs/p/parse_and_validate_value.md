# parse_and_validate_value

## Location
[src/backend/utils/misc/guc.c:3132-3344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L3132-L3344)

## Overview
Comprehensive validation function that parses and validates a proposed configuration parameter value according to its data type and built-in constraints.

## Definition

```c
struct config_generic *record,
						 const char *name, const char *value,
						 GucSource source, int elevel,
						 union config_var_val *newval, void **newextra)
{
	switch (record->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) record;

				if (!parse_bool(value, &newval->boolval))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("parameter \"%s\" requires a Boolean value",
									name)));
					return false;
				}

				if (!call_bool_check_hook(conf, &newval->boolval, newextra,
										  source, elevel))
					return false;
			}
			break;
		case PGC_INT:
			{
				struct config_int *conf = (struct config_int *) record;
				const char *hintmsg;

				if (!parse_int(value, &newval->intval,
							   conf->gen.flags, &hintmsg))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));
					return false;
				}

				if (newval->intval < conf->min || newval->intval > conf->max)
				{
					const char *unit = get_config_unit_name(conf->gen.flags);
					const char *unitspace;

					if (unit)
						unitspace = " ";
					else
						unit = unitspace = "";

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%d%s%s is outside the valid range for parameter \"%s\" (%d%s%s .. %d%s%s)",
									newval->intval, unitspace, unit,
									name,
									conf->min, unitspace, unit,
									conf->max, unitspace, unit)));
					return false;
				}

				if (!call_int_check_hook(conf, &newval->intval, newextra,
										 source, elevel))
					return false;
			}
			break;
		case PGC_REAL:
			{
				struct config_real *conf = (struct config_real *) record;
				const char *hintmsg;

				if (!parse_real(value, &newval->realval,
								conf->gen.flags, &hintmsg))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));
					return false;
				}

				if (newval->realval < conf->min || newval->realval > conf->max)
				{
					const char *unit = get_config_unit_name(conf->gen.flags);
					const char *unitspace;

					if (unit)
						unitspace = " ";
					else
						unit = unitspace = "";

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%g%s%s is outside the valid range for parameter \"%s\" (%g%s%s .. %g%s%s)",
									newval->realval, unitspace, unit,
									name,
									conf->min, unitspace, unit,
									conf->max, unitspace, unit)));
					return false;
				}

				if (!call_real_check_hook(conf, &newval->realval, newextra,
										  source, elevel))
					return false;
			}
			break;
		case PGC_STRING:
			{
				struct config_string *conf = (struct config_string *) record;

				/*
				 * The value passed by the caller could be transient, so we
				 * always strdup it.
				 */
				newval->stringval = guc_strdup(elevel, value);
				if (newval->stringval == NULL)
					return false;

				/*
				 * The only built-in "parsing" check we have is to apply
				 * truncation if GUC_IS_NAME.
				 */
				if (conf->gen.flags & GUC_IS_NAME)
					truncate_identifier(newval->stringval,
										strlen(newval->stringval),
										true);

				if (!call_string_check_hook(conf, &newval->stringval, newextra,
											source, elevel))
				{
					guc_free(newval->stringval);
					newval->stringval = NULL;
					return false;
				}
			}
			break;
		case PGC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) record;

				if (!config_enum_lookup_by_name(conf, value, &newval->enumval))
				{
					char	   *hintmsg;

					hintmsg = config_enum_get_options(conf,
													  "Available values: ",
													  ".", ", ");

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));

					if (hintmsg)
						pfree(hintmsg);
					return false;
				}

				if (!call_enum_check_hook(conf, &newval->enumval, newextra,
										  source, elevel))
					return false;
			}
			break;
	}

	return true;
}


/*
 * set_config_option: sets option `name' to given value.
 *
 * The value should be a string, which will be parsed and converted to
 * the appropriate data type.  The context and source parameters indicate
 * in which context this function is being called, so that it can apply the
 * access restrictions properly.
 *
 * If value is NULL, set the option to its default value (normally the
 * reset_val, but if source == PGC_S_DEFAULT we instead use the boot_val).
 *
 * action indicates whether to set the value globally in the session, locally
 * to the current top transaction, or just for the duration of a function call.
 *
 * If changeVal is false then don't really set the option but do all
 * the checks to see if it would work.
 *
 * elevel should normally be passed as zero, allowing this function to make
 * its standard choice of ereport level.  However some callers need to be
 * able to override that choice;
```
## Detailed Description
This static function serves as the central validation engine for PostgreSQL's configuration parameter system. It performs type-specific parsing and validation for all supported GUC parameter types: boolean, integer, real, string, and enum. 

The function uses a switch statement based on the parameter's vartype field to handle each data type appropriately:

- **Boolean parameters**: Uses parse_bool() and calls boolean-specific check hooks
- **Integer parameters**: Uses parse_int(), validates against min/max ranges, handles units, and calls integer-specific check hooks  
- **Real parameters**: Uses parse_real(), validates against min/max ranges, handles units, and calls real-specific check hooks
- **String parameters**: Uses guc_strdup() for memory allocation, applies identifier truncation if GUC_IS_NAME flag is set, and calls string-specific check hooks
- **Enum parameters**: Uses config_enum_lookup_by_name() for validation and provides helpful error messages listing available options

Each validation path includes comprehensive error reporting with appropriate error codes and hint messages. The function also invokes parameter-specific check hooks that allow custom validation logic.

## Parameters / Member Variables
- : Pointer to the GUC parameter's configuration record containing type information and constraints
- : The parameter name (used primarily for error reporting)
- : The proposed parameter value as a string to be parsed and validated
- : Identifies the source of the value (used by check hooks for context-specific validation)
- : Error reporting level (ERROR, WARNING, etc.)
- : Output parameter that receives the parsed and validated value in the appropriate type
- : Output parameter for additional data returned by parameter-specific check hooks (caller must initialize to NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](../c/config_generic.md), config_bool, config_int, config_real, config_string, config_enum (struct types)
  - config_var_val (union type)
  - [parse_bool](parse_bool.md), parse_int, parse_real (parsing functions)
  - [config_enum_lookup_by_name](../c/config_enum_lookup_by_name.md), config_enum_get_options (enum handling)
  - [call_bool_check_hook](../c/call_bool_check_hook.md), call_int_check_hook, call_real_check_hook, call_string_check_hook, call_enum_check_hook (validation hooks)
  - [guc_strdup](../g/guc_strdup.md), guc_free (memory management)
  - [get_config_unit_name](../g/get_config_unit_name.md) (unit formatting)
  - [truncate_identifier](../t/truncate_identifier.md) (identifier processing)
  - ereport, errcode, errmsg, errhint (error reporting)
- Called from (representative examples):
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md)
  - [set_config_option](../s/set_config_option.md) (via newval parameter)

## Notes and Other Information
- This is a static function, only accessible within the guc.c module
- Performs both syntactic parsing and semantic validation in a single operation
- Provides detailed error messages with hints for invalid values, especially for enum types
- Memory management is handled carefully - allocated strings are freed on validation failure
- The function integrates tightly with PostgreSQL's check hook system for extensible validation
- [Range](../R/Range.md) validation for numeric types includes proper unit formatting in error messages
- Essential component of PostgreSQL's configuration management infrastructure