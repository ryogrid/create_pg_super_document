# JsonbAggState

## Location
[src/backend/utils/adt/jsonb.c:36-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L36-L43)

## Overview
JsonbAggState is a state structure used by JSONB aggregate functions to maintain aggregation state and type conversion information during the aggregation process.

## Definition


## Detailed Description
JsonbAggState serves as the aggregation state structure for JSONB aggregate functions like jsonb_agg() and jsonb_object_agg(). It encapsulates the result construction state along with type categorization and output function information needed for proper conversion of input values to JSON representation. This structure is particularly important for object aggregation where both keys and values need type-specific handling during the aggregation process.

## Parameters / Member Variables
- : Pointer to JsonbInState that maintains the construction state and holds the aggregated result
- : JsonTypeCategory enum value indicating the JSON type category for keys (used in object aggregation)
- : OID of the output function used to convert key values to their string representation
- : JsonTypeCategory enum value indicating the JSON type category for values
- : OID of the output function used to convert values to their appropriate JSON representation

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbInState](JsonbInState.md)
  - JsonTypeCategory
  - Oid
- Called from (representative examples):
  - [jsonb_agg_transfn_worker](../j/jsonb_agg_transfn_worker.md)
  - [jsonb_agg_finalfn](../j/jsonb_agg_finalfn.md)
  - [jsonb_object_agg_transfn_worker](../j/jsonb_object_agg_transfn_worker.md)
  - [jsonb_object_agg_finalfn](../j/jsonb_object_agg_finalfn.md)

## Notes and Other Information
- Essential for implementing PostgreSQL's JSONB aggregation functions
- The key-related fields (key_category, key_output_func) are primarily used by jsonb_object_agg() for handling object keys
- The val-related fields handle value conversion for both jsonb_agg() and jsonb_object_agg()
- Type categorization enables proper JSON representation of different PostgreSQL data types during aggregation
- The output function OIDs are cached to avoid repeated function lookups during aggregation