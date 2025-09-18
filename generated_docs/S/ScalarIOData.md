# ScalarIOData

## Location
src/backend/utils/adt/jsonfuncs.c: 154 - 158

## Overview
ScalarIOData is a structure that caches type input/output metadata required for efficiently converting JSON scalar values to PostgreSQL data types.

## Definition


## Detailed Description
ScalarIOData serves as a caching structure that stores precompiled type conversion information needed for the populate_scalar() function. This structure optimizes the performance of JSON-to-PostgreSQL type conversions by avoiding repeated lookups of type input/output functions and their parameters. It contains the essential metadata required to convert JSON scalar values (strings, numbers, booleans, null) into their corresponding PostgreSQL data types.

## Parameters / Member Variables
- : Object identifier (Oid) representing the parameter required for the type input function
- : FmgrInfo structure containing cached function manager information for the type input function

## Dependencies
- Functions called/Symbols referenced:
  - (Uses built-in PostgreSQL types: Oid, FmgrInfo)
- Called from (representative examples):
  - ColumnIOData
  - JsObjectFree
  - populate_scalar

## Notes and Other Information
This structure is specifically designed for performance optimization in JSON processing scenarios where the same data types are converted repeatedly. By caching the type input function information (FmgrInfo) and its parameters (typioparam), the system avoids expensive function lookup operations during JSON-to-PostgreSQL type conversions. The structure is commonly used in conjunction with other JSON processing structures to maintain type conversion state across multiple scalar value conversions.