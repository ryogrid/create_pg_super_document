# generate_bitmap_or_paths

## Location
[src/backend/optimizer/path/indxpath.c:1180-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1180-L1286)

## Overview
 searches through restriction clauses to find OR clauses and generates BitmapOrPath nodes for each one that can be handled via bitmap index scans.

## Definition

```c
static List *
generate_bitmap_or_paths(PlannerInfo *root, RelOptInfo *rel,
						 List *clauses, List *other_clauses)
```
## Detailed Description
This recursive function processes OR clauses to create bitmap OR trees by:

1. **OR Clause Identification**: Scans through the clauses list looking for RestrictInfo nodes that contain OR expressions using .

2. **ARM Processing**: For each OR clause found, examines every arm (sub-expression) of the OR:
   - **AND Arms**: If an arm is an AND clause, extracts its arguments and calls  to find matching indexes, then recursively calls itself to handle any nested ORs
   - **Single Arms**: If an arm is a simple RestrictInfo, wraps it in a list and calls 

3. **Viability Check**: Ensures that every ARM of the OR clause can be matched to at least one index path - if any arm fails to match, the entire OR is abandoned.

4. **Path Selection**: For each viable ARM, uses  to select the most promising combination of index paths.

5. **BitmapOrPath Creation**: If all arms have viable paths, combines them into a BitmapOrPath using .

The function handles nested OR structures through recursion and uses both current and other clauses as context for index path generation, supporting complex WHERE clause structures.

## Parameters / Member Variables
- : PlannerInfo containing planner state and configuration  
- : RelOptInfo representing the relation being processed
- : List of restriction clauses to search for OR expressions
- : Additional clauses that provide context but aren't searched for ORs

## Dependencies
- Functions called/Symbols referenced:
  - [build_paths_for_OR](../b/build_paths_for_OR.md)
  - [choose_bitmap_and](../c/choose_bitmap_and.md)
  - [create_bitmap_or_path](../c/create_bitmap_or_path.md)
  - [restriction_is_or_clause](../r/restriction_is_or_clause.md)
  - [is_andclause](../i/is_andclause.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [create_index_paths](../c/create_index_paths.md)
  - [generate_bitmap_or_paths](generate_bitmap_or_paths.md) (recursive)

## Notes and Other Information
- Recursively handles nested OR expressions within AND clauses
- Requires ALL arms of an OR to have viable index paths - partial matches are rejected
- The function is self-recursive to handle arbitrarily deep OR/AND nesting
- Uses bitmap scan capabilities exclusively - not suitable for regular index scans
- The  combination provides fuller context for index path matching than either clause list alone