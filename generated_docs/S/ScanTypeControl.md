# ScanTypeControl

## Location
[src/backend/optimizer/path/indxpath.c:49-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L49-L56)

## Overview
 is an enumeration that controls the type of index scan strategy to be used during query planning, specifying whether to generate plain index scans, bitmap scans, or allow either type.

## Definition


## Detailed Description
This enumeration is used in PostgreSQL's query optimizer to control which type of index scan paths should be considered when building query execution plans. It acts as a directive to the path generation functions, indicating the scanning capabilities required from the index access method:

- **ST_INDEXSCAN**: Forces generation of paths that use traditional index scans, which require the access method to support the  interface for sequential tuple retrieval
- **ST_BITMAPSCAN**: Forces generation of paths that use bitmap index scans, which require the access method to support the  interface for bitmap-based tuple identification  
- **ST_ANYSCAN**: Allows the optimizer to consider both scan types, letting it choose the most appropriate method based on cost estimates and other factors

The enum is primarily used to restrict path generation based on the specific requirements of different query contexts, ensuring that only compatible scan methods are considered for a given index and query pattern.

## Parameters / Member Variables
- : Indicates that only plain index scan paths should be generated, requiring  support
- : Indicates that only bitmap index scan paths should be generated, requiring  support  
- : Indicates that both plain and bitmap index scan paths may be generated, allowing flexibility in scan method selection

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (used in related data structures)
- Called from (representative examples):
  - [build_index_paths](../b/build_index_paths.md) (at src/backend/optimizer/path/indxpath.c:807)
  - ec_member_matches_arg (at src/backend/optimizer/path/indxpath.c:108)

## Notes and Other Information
- This enum is defined locally within  and is used specifically for index path generation logic
- The choice between different scan types depends on factors such as selectivity, index size, and the access method's capabilities
- Bitmap scans are generally preferred for low-selectivity queries where many tuples match the index condition
- Plain index scans are typically more efficient for high-selectivity queries or when ordering is important
- The enum works in conjunction with access method interface checking to ensure generated paths are executable by the chosen index access method