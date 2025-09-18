# jsonb_object_agg_transfn

## Location
src/backend/utils/adt/jsonb.c: 1896 - 1905

## Overview
Transition function for standard JSONB object aggregation that collects key-value pairs into a JSONB object including NULL values.

## Definition


## Detailed Description
This function serves as the transition function for the  aggregate function, which collects key-value pairs into a JSONB object. It acts as a wrapper around , passing  for both  and  parameters. This means NULL values are included in the object and duplicate keys are allowed (later values overwrite earlier ones). The function is called once for each input row during aggregate processing.

## Parameters / Member Variables
- : Function call information structure containing the aggregate state and input key/value pair

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_object_agg_transfn_worker](jsonb_object_agg_transfn_worker.md)
- Called from (representative examples):
  - PostgreSQL aggregate framework (no direct code references found)

## Notes and Other Information
- This function cannot be called directly due to its internal-type argument requirement
- The function delegates all processing to  with 
- Allows NULL values in the resulting object
- Does not enforce key uniqueness (duplicate keys result in value overwriting)
- Part of PostgreSQL's JSONB aggregate function family located in src/backend/utils/adt/jsonb.c:1896-1905