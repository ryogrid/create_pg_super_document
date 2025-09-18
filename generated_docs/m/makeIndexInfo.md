# makeIndexInfo

## Location
src/backend/nodes/makefuncs.c: 808 - 863

## Overview
Creates and initializes an IndexInfo node structure that contains comprehensive metadata about a database index for use during index operations and query processing.

## Definition


## Detailed Description
This function constructs a complete IndexInfo structure that serves as the primary metadata container for database indexes in PostgreSQL. The IndexInfo structure contains all essential information needed for index creation, maintenance, and utilization during query execution. It handles both regular indexes and specialized types like partial indexes (with predicates), expression indexes, and unique indexes.

The function initializes all fields of the IndexInfo structure, including runtime state fields that will be populated during index operations. It also performs validation checks to ensure the parameter combinations are valid (e.g., summarizing indexes cannot have non-key attributes).

## Parameters / Member Variables
- : Total number of attributes (columns) in the index, including both key and non-key attributes
- : Number of key attributes in the index (must be > 0 and ≤ numattrs)
- : Object ID of the access method (index type) to be used
- : List of expressions for expression-based index columns (NULL for simple column indexes)
- : List of predicate expressions for partial indexes (NULL for complete indexes)
- : Boolean flag indicating whether this is a unique index
- : Boolean flag for unique indexes - whether NULL values should be considered distinct
- : Boolean indicating whether the index is ready for inserts
- : Boolean indicating whether this is a concurrent index operation
- : Boolean indicating whether this is a summarizing index (cannot have non-key attributes)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a new node of type IndexInfo
  -  - The main index metadata structure type
  -  - Global variable for current memory allocation context
- Called from (representative examples):
  -  - Main index creation command handler
  -  - Builds IndexInfo from catalog data
  -  - Index compatibility validation
  -  - Concurrent index creation

## Notes and Other Information
- Part of the node creation utilities in PostgreSQL's backend
- The function initializes many fields to default values (NULL, false, 0) that will be populated later during index operations
- Includes validation assertions to ensure parameter consistency
- Sets up memory context tracking for proper cleanup
- Essential for all index-related operations in PostgreSQL's execution engine
- The structure supports advanced index features like exclusion constraints, speculative inserts, and parallel index building