# create_sort_plan

## Location
[src/backend/optimizer/plan/createplan.c:2181-2214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2181-L2214)

## Overview
Creates a Sort plan node from a SortPath, recursively building plans for subpaths and optimizing the target list for efficient sorting operations.

## Definition

```c
static Sort *
create_sort_plan(PlannerInfo *root, SortPath *best_path, int flags)
```
## Detailed Description
The  function generates a Sort plan node from the provided SortPath. It's a key component in PostgreSQL's query planning process, responsible for creating execution plans that perform sorting operations. The function recursively creates plans for subpaths and applies specific optimizations for sorting.

The function requests a smaller target list (CP_SMALL_TLIST flag) to avoid carrying excess columns through the sorting process, which improves performance by reducing memory usage and I/O overhead. Since Sort nodes don't perform projection themselves, target list requirements are passed through to the underlying plan.

Special handling is implemented for child relations in inheritance hierarchies, ensuring that equivalence class members are properly resolved when creating sort keys from path keys.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information and context
- : SortPath structure representing the chosen sorting strategy and its properties
- : Integer bitmask controlling plan creation behavior (e.g., CP_SMALL_TLIST for optimization)

## Dependencies
- Functions called/Symbols referenced:
  - : Recursively creates execution plans for subpaths
  - : Constructs Sort node from pathkey specifications
  - : Macro to check if a relation is a child relation
  - : Copies common path information to the plan node
- Called from (representative examples):
  - : Main plan creation dispatch function

## Notes and Other Information
- This is a static function, meaning it's only accessible within the createplan.c compilation unit
- The CP_SMALL_TLIST flag optimization is crucial for Sort performance as it eliminates unnecessary columns early
- Special relid handling for child relations ensures correct equivalence class member resolution in inheritance scenarios
- The function preserves all generic path information (costs, parallel safety, etc.) in the resulting plan node
- Sort nodes are non-projecting, meaning they pass through their input unchanged except for ordering