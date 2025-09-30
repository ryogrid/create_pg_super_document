# restriction_is_or_clause

## Location
[src/backend/optimizer/util/restrictinfo.c:416-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L416-L430)

## Overview
Simple predicate function that determines whether a RestrictInfo node contains an OR clause by checking the orclause field.

## Definition

```c
bool
restriction_is_or_clause(RestrictInfo *restrictinfo)
```
## Detailed Description
This function provides a simple boolean test to determine if a RestrictInfo contains an OR clause. It works by examining the orclause field of the RestrictInfo structure - if this field is non-NULL, it indicates that the RestrictInfo was created to represent an OR clause with RestrictInfo nodes inserted above each OR constituent. This is a lightweight utility function used throughout the PostgreSQL optimizer to identify OR clauses for special processing such as bitmap index scans, OR clause extraction, and optimization path generation.

## Parameters / Member Variables
- : The RestrictInfo node to examine for OR clause content

## Dependencies
- Functions called/Symbols referenced: (none - direct field access only)
- Called from (representative examples):
  - [generate_bitmap_or_paths](../g/generate_bitmap_or_paths.md)
  - [match_join_clauses_to_index](../m/match_join_clauses_to_index.md)
  - [TidQualFromRestrictInfoList](../T/TidQualFromRestrictInfoList.md)
  - [remove_rel_from_restrictinfo](remove_rel_from_restrictinfo.md)
  - [restriction_is_always_true](restriction_is_always_true.md)
  - [restriction_is_always_false](restriction_is_always_false.md)
  - [extract_restriction_or_clauses](../e/extract_restriction_or_clauses.md)
  - [extract_or_clause](../e/extract_or_clause.md)
  - make_simple_restrictinfo

## Notes and Other Information
- Simple implementation: The function directly tests the orclause field rather than parsing the actual clause structure, making it very efficient
- OR clause identification: The orclause field is populated during RestrictInfo creation when make_sub_restrictinfos processes OR expressions
- Optimization usage: This function is frequently used in path generation and index optimization to identify opportunities for bitmap OR scans and other OR-specific optimizations
- Field semantics: A non-NULL orclause indicates that the RestrictInfo represents an OR clause where each constituent has been wrapped with its own RestrictInfo node
- Return value: Returns true if the RestrictInfo contains an OR clause, false otherwise

## Simplified Source

```c
bool
restriction_is_or_clause(RestrictInfo *restrictinfo)
{
    return (restrictinfo->orclause != NULL);
}
```