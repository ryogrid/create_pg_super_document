# lengthCompareJsonbPair

## Location
[src/backend/utils/adt/jsonb_util.c:1925-1948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1925-L1948)

## Overview
A qsort_arg() comparator function that compares JsonbPair values based on their key lengths, used for sorting JSON object key-value pairs in PostgreSQL's JSONB implementation.

## Definition


## Detailed Description
This function serves as a comparison function for sorting JsonbPair structures using qsort_arg(). It performs "length-wise" string comparisons on the keys of JsonbPair objects, meaning it compares strings by their length rather than lexicographically. The function is designed to maintain stability for equal pairs by respecting their original order field, ensuring that when keys are equal, the pair with the smaller order value comes first. This behavior is crucial for the uniqueification algorithm, which prefers the first element when duplicates are found.

The function also provides an optional binary equality check through the binequal parameter, allowing callers to determine if two pairs have complete binary equality rather than just equivalent keys.

## Parameters / Member Variables
- : Pointer to the first JsonbPair to compare (cast from void*)
- : Pointer to the second JsonbPair to compare (cast from void*)  
- : Optional pointer to a bool that will be set to true if the pairs have full binary equality

## Dependencies
- Functions called/Symbols referenced:
  - [lengthCompareJsonbStringValue](lengthCompareJsonbStringValue.md)
  - [JsonbPair](../J/JsonbPair.md) (struct type)
- Called from (representative examples):
  - [uniqueifyJsonbObject](../u/uniqueifyJsonbObject.md)

## Notes and Other Information
- This is a static function within jsonb_util.c, not exposed to other modules
- The comparison is "length-wise" rather than lexicographic, which is important for JSONB's internal ordering semantics
- The function guarantees stable sorting by using the order field as a tie-breaker when keys are equal
- Used specifically in the JSONB object uniqueification process to sort pairs before removing duplicates
- The binequal parameter is optional and may be NULL if the caller doesn't need binary equality information