# convert_tuples_by_name

## Location
[src/backend/access/common/tupconvert.c:102-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupconvert.c#L102-L123)

## Overview
Creates a tuple conversion map by matching input and output columns by their names, intended for use with rowtypes related by inheritance.

## Definition


## Detailed Description
This function sets up tuple conversion infrastructure for cases where tuples need to be converted between different tuple descriptors and the correspondence is determined by matching column names rather than positions. It's specifically designed for use with rowtypes that are related by inheritance, where an exact match of both type and typmod is expected.

The function acts as a convenience wrapper that first calls  to create the attribute mapping, then delegates to  to complete the conversion map setup if needed.

## Parameters
- : Input tuple descriptor defining the source tuple structure
- : Output tuple descriptor defining the target tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [convert_tuples_by_name_attrmap](convert_tuples_by_name_attrmap.md)
  - [AttrMap](../A/AttrMap.md) (struct)
  - TupleConversionMap (struct)
- Called from (representative examples):
  - [acquire_inherited_sample_rows](../a/acquire_inherited_sample_rows.md)
  - [ExecEvalConvertRowtype](../E/ExecEvalConvertRowtype.md)
  - [ExecGetChildToRootMap](../E/ExecGetChildToRootMap.md)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)

## Notes and Other Information
- Dropped columns are ignored in both input and output descriptors during name-based matching
- Designed for inheritance-related tuple conversions where exact type and typmod matching is expected
- Returns NULL if no runtime conversion is needed (descriptors are compatible)
- Error messages may be unhelpful unless both rowtypes are named composite types
- More flexible than position-based conversion as it handles column reordering