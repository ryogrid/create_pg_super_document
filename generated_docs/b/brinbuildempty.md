# brinbuildempty

## Location
[src/backend/access/brin/brin.c:1264-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1264-L1292)

## Overview
Creates an empty BRIN index structure consisting of only a properly initialized metadata page.

## Definition

```c
struct */
	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);
```
## Detailed Description
 is responsible for creating the minimal structure for an empty BRIN index. This function is typically called during index creation when no initial data needs to be indexed, or as part of index recreation operations. 

The function performs the following operations:
1. **Buffer Extension**: Extends the index relation with a new buffer for the metadata page using the  (initialization fork)
2. **Critical Section**: Enters a critical section to ensure atomic operations
3. **Metadata Initialization**: Initializes the metadata page with current BRIN version and pages-per-range configuration
4. **WAL Logging**: Logs the new page to the write-ahead log using  for crash recovery
5. **Buffer Management**: Marks the buffer as dirty and releases it

The resulting empty BRIN index contains only the essential metadata page and is ready for future tuple insertions.

## Parameters / Member Variables
- : The BRIN index relation to initialize as empty

## Dependencies
- Functions called/Symbols referenced:
  - : Extend relation with new buffer
  - : Get buffered relation from index relation
  - : Initialize BRIN metadata page
  - : Get pages per range configuration
  - : Mark buffer as modified
  - : Log new page for WAL
  - : Release and unlock buffer
- Called from (representative examples):
  - : BRIN access method handler function

## Notes and Other Information
- Creates the minimal viable BRIN index structure (metadata page only)
- Uses critical section to ensure atomicity of metadata page creation
- Uses  instead of  for initialization
- WAL logging with  ensures crash recovery consistency
- The  and  flags optimize buffer extension
- Complements  function which builds populated indexes