# CustomPath

## Location
src/include/nodes/pathnodes.h: 1905 - 1914

## Overview
CustomPath represents an extensible path node that allows external extensions to implement custom scan methods and integrate them into PostgreSQL's query planning system.

## Definition


## Detailed Description
CustomPath is part of PostgreSQL's extensibility framework, enabling third-party extensions to implement custom scanning strategies that integrate seamlessly with the core query planner. This path type provides a flexible mechanism for extensions to define their own access methods, optimization strategies, and execution plans while leveraging PostgreSQL's existing cost-based optimization infrastructure.

The CustomPath supports hierarchical planning through child paths, making it suitable for complex operations that may need to compose multiple scan strategies. Extensions can store private data and provide callback methods for plan creation and reparameterization, allowing full control over both planning and execution phases.

This extensibility mechanism is used by various PostgreSQL extensions for specialized data access patterns, parallel processing frameworks, and integration with external systems that require custom optimization strategies.

## Parameters / Member Variables
- : Base Path structure containing standard path information including pathtype, parent relation, costs, and execution properties
- : Bitmask of CUSTOMPATH_* flags defined in nodes/extensible.h that control various aspects of custom path behavior and capabilities
- : List of child Path nodes that this custom path depends on, enabling hierarchical planning and composite operations
- : List of RestrictInfo nodes containing restriction clauses specific to this custom path
- : List containing extension-private data needed for plan creation and execution, passed from planning to execution time
- : Pointer to CustomPathMethods structure containing callback functions for plan creation and reparameterization

## Dependencies
- Functions called/Symbols referenced:
  - Path (base structure)
  - CustomPathMethods (method callbacks)
  - List (for child paths and private data)
- Called from (representative examples):
  - create_customscan_plan (converts CustomPath to execution plan)
  - ExecSupportsMarkRestore (checks execution capabilities)
  - is_projection_capable_path (determines projection capabilities)

## Notes and Other Information
- Part of PostgreSQL's extensibility API for custom scan providers
- Extensions must provide CustomPathMethods with PlanCustomPath callback
- Supports complex hierarchical operations through custom_paths
- Can be reparameterized through ReparameterizeCustomPathByChild callback
- Used by extensions like pg_hint_plan, postgres_fdw, and parallel processing extensions
- Flags control behavior like support for backward scanning, mark/restore operations
- Custom providers are responsible for accurate cost estimation
- Integrates with PostgreSQL's parallel query execution framework
- Allows extensions to implement domain-specific optimizations not available in core PostgreSQL