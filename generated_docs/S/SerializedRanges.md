# SerializedRanges

## Location
src/backend/access/brin/brin_minmax_multi.c: 205 - 220

## Overview
SerializedRanges is the on-disk storage representation of BRIN minmax-multi index summaries, stored as a bytea value with a varlena header for persistent storage in PostgreSQL.

## Definition


## Detailed Description
SerializedRanges is the persistent storage format for BRIN minmax-multi index summaries, designed for efficient disk storage and retrieval. It serves as the counterpart to the in-memory Ranges structure, using a compact bytea representation with a varlena header for integration with PostgreSQL's variable-length storage system. The structure stores essential metadata (type information, counts) in the header, followed by a flexible data array containing the actual serialized boundary values and single-point values. The serialization process is handled by brin_range_serialize/brin_range_deserialize functions.

## Parameters / Member Variables
- `vl_len_`: Standard varlena header containing the total length of the serialized structure; managed automatically by PostgreSQL's varlena system
- `typid`: Object identifier of the data type being indexed, preserved for type safety during deserialization
- `nranges`: Number of regular ranges stored in the serialized data (each range represents a min/max pair)
- `nvalues`: Total number of values in the data array, including both range boundaries and single-point values
- `maxvalues`: Maximum number of values allowed as specified by the values_per_range reloption
- `data[FLEXIBLE_ARRAY_MEMBER]`: Flexible array containing the serialized boundary values and single-point values in a compact binary format

## Dependencies
- Constants referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Used by functions:
  - [range_deduplicate_values](../r/range_deduplicate_values.md)
  - [brin_range_serialize](../b/brin_range_serialize.md)/deserialize
  - [brin_minmax_multi_serialize](../b/brin_minmax_multi_serialize.md)
  - [brin_minmax_multi_add_value](../b/brin_minmax_multi_add_value.md)
  - [brin_minmax_multi_consistent](../b/brin_minmax_multi_consistent.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)
  - [brin_minmax_multi_summary_out](../b/brin_minmax_multi_summary_out.md)

## Notes and Other Information
- Designed as the on-disk counterpart to the in-memory Ranges structure
- Uses PostgreSQL's varlena format for seamless integration with bytea storage
- Serialization/deserialization maintains the same logical layout as Ranges (ranges first, then single points)
- The data array contains binary-serialized values whose format depends on the indexed data type
- Part of the two-tier system: Ranges for processing, SerializedRanges for storage
- Critical for BRIN index persistence and cross-session consistency
- The constraint (2*nranges + nvalues) <= maxvalues ensures storage bounds are respected