# json_build_array_worker

## Location
[src/backend/utils/adt/json.c:1335-1364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1335-L1364)

## Overview
The json_build_array_worker function is the core implementation that constructs a JSON array from an array of PostgreSQL Datum values with their corresponding type and null information.

## Definition
```c
Datum json_build_array_worker(int nargs, const Datum *args, const bool *nulls, const Oid *types, bool absent_on_null)
```

## Detailed Description
This function performs the actual work of constructing a JSON array from PostgreSQL values. It iterates through the provided arguments, converts each to its JSON representation using the add_json function, and assembles them into a properly formatted JSON array string enclosed in square brackets. The function supports an absent_on_null option that allows null values to be omitted from the resulting array rather than being included as JSON null values.

## Parameters / Member Variables
- nargs: The number of arguments to process
- args: Array of Datum values to be converted to JSON
- nulls: Array of boolean flags indicating which arguments are null
- types: Array of PostgreSQL type OIDs for each argument
- absent_on_null: If true, null values are omitted from the array; if false, they are included as JSON null

## Dependencies
- Functions called/Symbols referenced:
  - [makeStringInfo](../m/makeStringInfo.md): Creates a new StringInfo buffer for building the result
  - [add_json](../a/add_json.md): Converts individual PostgreSQL values to their JSON representation
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md): Converts the final C string to PostgreSQL text type
  - [appendStringInfoChar](../a/appendStringInfoChar.md): Appends a single character to the StringInfo buffer
  - [appendStringInfoString](../a/appendStringInfoString.md): Appends a string to the StringInfo buffer
  - [PointerGetDatum](../P/PointerGetDatum.md): Converts a pointer to a Datum value

- Called from (representative examples):
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md): Used in expression evaluation for JSON constructors
  - [json_build_array](json_build_array.md): Called by the main json_build_array SQL function
  - Referenced in JSON_H header file

## Notes and Other Information
- The function builds the JSON array incrementally using a StringInfo buffer for efficiency
- Array elements are separated by ", " (comma followed by space)
- The absent_on_null parameter provides flexible handling of null values in arrays
- This is a worker function that handles the core logic for JSON array construction
- Located in src/backend/utils/adt/json.c:1335-1364
- Used both by SQL functions and internal PostgreSQL expression evaluation