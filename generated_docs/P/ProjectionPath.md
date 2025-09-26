# ProjectionPath

## Location
[src/include/nodes/pathnodes.h:2173-2178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2173-L2178)

## Overview
ProjectionPath represents a projection operation (targetlist computation) in PostgreSQL's query planner, handling column selection and expression evaluation that may or may not require a separate Result plan node.

## Definition
```c
typedef struct ProjectionPath
{
    Path        path;
    Path       *subpath;      /* path representing input source */
    bool        dummypp;      /* true if no separate Result is needed */
} ProjectionPath;
```

## Detailed Description
ProjectionPath encapsulates projection operations in the query execution plan, which involve computing a specific targetlist (set of output columns and expressions) from input data. The key insight behind ProjectionPath is optimization flexibility: sometimes projection can be integrated directly into the underlying plan node (avoiding the overhead of a separate Result node), while other times a distinct Result node is necessary.

When dummypp is true, it indicates that the projection work can be \pushed down\ into the input plan node by modifying its output targetlist directly. When false, a separate Result plan node will be generated to perform the projection. This design allows the planner to optimize projection costs while maintaining planning flexibility.

The path is used throughout the planning process to represent computed columns, mathematical expressions, function calls, and column selection operations that transform the raw input data into the desired output format.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information like cost estimates, output cardinality, and path target
- `subpath`: Pointer to the input path that provides the source data for projection
- `dummypp`: Boolean flag indicating whether this is a \dummy\ projection path that can be optimized away by integrating the projection into the subpath's plan node (true) or requires a separate Result node (false)

## Dependencies
- Functions called/Symbols referenced:
  - Path (inherited base structure)
- Called from (representative examples):
  - create_projection_path (path creation)
  - create_projection_plan (plan generation)
  - is_dummy_rel (optimization checks)
  - mark_async_capable_plan (async execution planning)

## Notes and Other Information
- ProjectionPath extends the base Path structure to add projection-specific information
- The dummypp optimization is crucial for performance as it can eliminate unnecessary plan nodes when projection can be done \for free\ by the input node
- Used extensively in SELECT statement processing where column selection, computed expressions, and function calls need to be evaluated
- The planner's cost model accounts for whether a Result node will actually be needed when estimating ProjectionPath costs
- Essential for handling complex SELECT clauses with expressions, function calls, and column transformations