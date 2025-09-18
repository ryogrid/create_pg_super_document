# SetOpStatePerGroup

## Location
[src/include/nodes/execnodes.h:2779-2780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2779-L2780)

## Overview
SetOpStatePerGroup is a pointer type to SetOpStatePerGroupData, representing per-group working state for set operations like UNION, INTERSECT, and EXCEPT in PostgreSQL's executor.

## Definition


## Detailed Description
SetOpStatePerGroup is a typedef that creates a pointer type to SetOpStatePerGroupData structures. It serves as an abstraction for managing per-group state information during set operations. The actual structure (SetOpStatePerGroupData) is defined privately in nodeSetOp.c and contains counters for tracking duplicate tuples from left and right inputs. This design allows SetOp nodes to efficiently handle both sorted and hashed execution strategies while maintaining clean separation between the public interface and internal implementation details.

## Parameters / Member Variables
This is a pointer type, so it points to SetOpStatePerGroupData which contains:
- : Number of left-input duplicates in the current group
- : Number of right-input duplicates in the current group

## Dependencies
- Functions called/Symbols referenced:
  - [SetOpStatePerGroupData](SetOpStatePerGroupData.md)
- Called from (representative examples):
  - initialize_counts
  - advance_counts
  - set_output_count
  - [setop_retrieve_direct](../s/setop_retrieve_direct.md)
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md)

## Notes and Other Information
- The actual structure definition is private to nodeSetOp.c, promoting encapsulation
- Used in both SETOP_SORTED and SETOP_HASHED execution modes
- In sorted mode, only one instance is kept in the plan state node
- In hashed mode, the hash table contains one instance per tuple group
- Essential for implementing SQL set operations with proper duplicate handling