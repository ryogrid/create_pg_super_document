# initGinState

## Location
[src/backend/access/gin/ginutil.c:97-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L97-L225)

## Overview
Initializes a GinState structure with index-specific information, including tuple descriptors, operator class functions, and collation settings for each indexed column.

## Definition

```c
void
initGinState(GinState *state, Relation index)
```
## Detailed Description
The  function fills in an empty  structure with all the necessary information to work with a specific GIN index. This includes setting up tuple descriptors, loading operator class support functions, and configuring collation information for each indexed column. The function handles both single-column and multi-column indexes differently, creating appropriate tuple descriptors for internal GIN storage format.

For single-column indexes, it uses the original tuple descriptor directly. For multi-column indexes, it creates a special 2-attribute tuple descriptor where the first attribute is an INT2 (column number) and the second attribute matches the original column's type.

The function loads various operator class support functions:
- Compare functions (for sorting keys)
- Extract value/query functions (for extracting searchable keys)
- Consistent/tri-consistent functions (for query matching)
- Compare partial functions (for partial matching support)

## Parameters / Member Variables
- : Pointer to the GinState structure to be initialized
- : The relation representing the GIN index being initialized

## Dependencies
- Functions called/Symbols referenced:
  -  (memory initialization)
  -  (get tuple descriptor)
  -  (create tuple descriptor)
  -  (initialize tuple descriptor entries)
  -  (set collation for attributes)
  -  and  (get operator class functions)
  -  (copy function manager info)
  -  (get type information)
  -  (format type names for error messages)
  - Various GIN procedure constants: , , , , , 

- Called from:
  -  (src/backend/access/gin/ginfast.c:1079)
  -  (src/backend/access/gin/gininsert.c:335)
  -  (src/backend/access/gin/gininsert.c:499)
  -  (src/backend/access/gin/ginscan.c:45)
  -  (src/backend/access/gin/ginvacuum.c:582)
  -  (src/backend/access/gin/ginvacuum.c:706, 719)

## Notes and Other Information
- All subsidiary data is allocated in the CurrentMemoryContext
- The function handles missing compare functions by looking up the default btree comparator for the data type
- At least one of consistent or tri-consistent functions must be provided by the operator class
- Partial matching support is optional and detected by the presence of compare partial function
- For collation handling, the function uses the index's specified collation or defaults to DEFAULT_COLLATION_OID
- The function performs extensive error checking to ensure all required operator class functions are available
- The  flag optimizes handling for single-column indexes by reusing the original tuple descriptor