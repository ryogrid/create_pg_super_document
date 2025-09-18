# JsonbValueInitNumericDatum

## Location
src/backend/utils/adt/jsonpath_exec.c: 3131 - 3140

## Overview
Initializes a JsonbValue structure with a numeric value from a given PostgreSQL Datum.

## Definition


## Detailed Description
This is a static utility function in the JSON path execution module that initializes a JsonbValue structure to represent a numeric value. It sets the JsonbValue type to jbvNumeric and stores the numeric data by extracting it from a PostgreSQL Datum using DatumGetNumeric. This function is used internally during JSON path operations when numeric values need to be converted from PostgreSQL's internal Datum representation to JsonbValue format for JSON processing.

## Parameters / Member Variables
- : Pointer to the JsonbValue structure to initialize
- : PostgreSQL Datum containing the numeric value to extract and store

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetNumeric (PostgreSQL macro to extract Numeric from Datum)
  - jbvNumeric (JsonbValue type constant for numeric values)
- Called from (representative examples):
  - JsonItemFromDatum (multiple locations in jsonpath_exec.c)

## Notes and Other Information
- This is a static helper function, only accessible within the jsonpath_exec.c module
- Part of the JSON path execution infrastructure for handling numeric data types
- Used during conversion of PostgreSQL native numeric types to JSONB representation
- The function assumes the input Datum actually contains a valid numeric value