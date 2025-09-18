# json_populate_record

## Location
src/backend/utils/adt/jsonfuncs.c: 2493 - 2499

## Overview
Populates fields of an existing PostgreSQL record using values from a JSON object, creating a new record with updated field values.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that takes an existing record and a JSON object as input, then creates a new record by populating/updating the record's fields with corresponding values from the JSON object. This function is a thin wrapper around the  function, specifically configured for JSON input with a record argument template.

The function operates by:
1. Accepting an existing record (as the first argument) and a JSON object (as the second argument)
2. Extracting field values from the JSON object that match field names in the record
3. Creating a new record with the original values updated/populated from the JSON
4. Performing automatic type conversion as needed

Unlike , this function requires an existing record as a template and updates its fields rather than creating a record type from scratch.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure containing:
  - Record argument (first parameter) - the template record to populate
  - JSON input argument (second parameter) - the JSON object containing new field values
  - Function context and metadata
  - Output type information

## Dependencies
- Functions called/Symbols referenced:
  -  (the main implementation function)
- Called from (representative examples):
  - SQL queries using  function calls
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This function is part of PostgreSQL's JSON/JSONB support system
- The actual work is delegated to  with parameters:
  - : "json_populate_record"
  - : true (indicating JSON input, not JSONB)
  - : true (has a record template argument)
  - : NULL (no soft error context)
- Located in 
- Differs from  in that it requires and updates an existing record template
- The function signature in SQL is: 
- Fields not present in the JSON object retain their original values from the template record