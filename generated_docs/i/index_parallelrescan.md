# index_parallelrescan

## Location
[src/backend/access/index/indexam.c:523-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L523-L540)

## Overview
The  function (re)starts a parallel scan of an index, performing necessary cleanup and reinitialization to begin or restart parallel index scanning operations.

## Definition


## Detailed Description
This function is responsible for reinitializing a parallel index scan. It performs two main operations:
1. Resets the heap fetch state if heap fetching is enabled for the scan
2. Calls the access method's parallel rescan function if one is provided

The function first checks if heap fetching is active () and resets the table index fetch state if necessary. Then it delegates to the access method's specific parallel rescan implementation (), which is optional - if the access method doesn't provide this function, the operation is treated as a no-op.

This function is part of PostgreSQL's parallel query execution infrastructure, allowing multiple worker processes to coordinate when rescanning an index.

## Parameters / Member Variables
- : An IndexScanDesc structure representing the index scan descriptor that needs to be reinitialized for parallel processing

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for scan validation)
  -  (resets heap fetch state)
  -  (type definition)
- Called from (representative examples):
  -  (src/backend/executor/nodeIndexonlyscan.c:767)
  -  (src/backend/executor/nodeIndexscan.c:1700)

## Notes and Other Information
- The parallel rescan functionality is optional for access methods - if  is NULL, no access method-specific reinitialization is performed
- The function assumes that the index scan descriptor is valid and properly initialized
- This is part of PostgreSQL's dynamic shared memory (DSM) based parallel query execution system
- Location: src/backend/access/index/indexam.c:523-540