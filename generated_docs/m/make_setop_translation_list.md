# make_setop_translation_list

## Location
[src/backend/optimizer/prep/prepjointree.c:1621-1658](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L1621-L1658)

## Overview
Builds translation lists that map parent query variables to child query variables for UNION ALL members, establishing the column correspondence needed for append relation optimization.

## Definition


## Detailed Description
This function constructs the variable translation mechanism required when optimizing UNION ALL operations through append relations. It creates two complementary data structures:

1. **Forward translation list** (): A list of Var nodes that represent how parent query references should be translated to reference the child query. Initially populated with simple variable references, but these get replaced with actual pulled-up expressions if the subquery optimization succeeds.

2. **Reverse translation array** (): An array that maps from child column numbers to parent column numbers, allowing reverse lookups during query processing.

The function processes the target list of the child query, creating Var nodes for each non-junk column that reference the specified range table entry number (). Junk columns (used for internal processing but not part of the final result) are skipped in the forward translation but still allocated space in the reverse translation array.

This translation mechanism is essential for PostgreSQL's append relation optimization, which allows UNION ALL queries to be processed more efficiently by treating them as a single relation with multiple data sources.

## Parameters / Member Variables
- : The child Query node whose target list will be processed to create the translation mappings
- : The range table index that the translated variables should reference (typically the child RTE index)  
- : The AppendRelInfo structure that will be populated with the translation information

## Dependencies
- Functions called/Symbols referenced:
  - list_length
  - [palloc0](../p/palloc0.md)
  - makeVarFromTargetEntry
  - lappend
  - lfirst (macro for list traversal)
- Called from (representative examples):
  - [pull_up_union_leaf_queries](../p/pull_up_union_leaf_queries.md)

## Notes and Other Information
- The function is static, limiting its scope to the prepjointree.c compilation unit
- The reverse translation array is initialized with zeros using palloc0, with entries for junk columns remaining zero
- The forward translation initially contains simple Var references, but these may be replaced with complex expressions during subsequent subquery pull-up operations
- Column numbering follows PostgreSQL's 1-based indexing convention (resno - 1 for array indexing)
- This function is a critical component in PostgreSQL's set operation optimization pipeline, enabling efficient processing of UNION ALL queries