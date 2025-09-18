# GetJsonPathVar

## Location
src/backend/utils/adt/jsonpath_exec.c: 2991 - 3035

## Overview
Returns the computed value of a JSON path variable with a given name by searching through a list of available variables and converting the found value to a JsonbValue.

## Definition


## Detailed Description
This function serves as a variable resolver in the jsonpath execution engine. It searches through a list of JsonPathVariable structures to find a variable matching the specified name and length. When found, it converts the variable's stored Datum value to a JsonbValue using JsonItemFromDatum. The function also handles NULL variables appropriately and provides both the resolved value and metadata about the variable's position and base object information for further processing.

## Parameters / Member Variables
- : void pointer that is cast to a List of JsonPathVariable structures containing available variables
- : char pointer to the name of the variable to look up (not null-terminated)
- : int specifying the length of the variable name
- : JsonbValue pointer where the base object value will be stored (output parameter)
- : int pointer where the variable's ID will be stored, or -1 if not found (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (extracts list cell content)
  - strncmp (compares variable names)
  - palloc (allocates memory for result)
  - JsonItemFromDatum (converts Datum to JsonbValue)
- Data types used:
  - JsonPathVariable (structure holding variable information)
  - List, ListCell (PostgreSQL list structures)
  - JsonbValue (JSON value representation)
  - jbvNull (null JSON value type)
- Called from (representative examples):
  - RETURN_ERROR macro in jsonpath_exec.c:312
  - JsonPathExists in jsonpath_exec.c:3893
  - JsonPathQuery in jsonpath_exec.c:3922
  - JsonPathValue in jsonpath_exec.c:4013
  - JsonTableResetRowPattern in jsonpath_exec.c:4267

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- Returns NULL if the variable is not found, with baseObjectId set to -1
- For NULL variables, sets baseObjectId to 0 and creates a jbvNull JsonbValue
- The function assigns sequential IDs starting from 1 to variables as it searches
- Memory is allocated for the result JsonbValue using palloc
- Used by higher-level jsonpath functions like JsonPathExists, JsonPathQuery, and JsonPathValue
- Part of PostgreSQL's SQL/JSON path expression variable resolution system