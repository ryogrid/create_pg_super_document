# ginrescan

## Location
[src/backend/access/gin/ginscan.c:490-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginscan.c#L490-L505)

## Overview
Resets and reinitializes a GIN index scan with new scan keys, allowing the same scan descriptor to be reused for a new query.

## Definition
```c
void ginrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys, ScanKey orderbys, int norderbys)
```

## Detailed Description
The `ginrescan` function provides the ability to restart a GIN index scan with different scan keys without having to completely tear down and recreate the scan infrastructure. This is an optimization that allows the same IndexScanDesc to be reused for multiple related queries or when scan parameters change during execution.

The function performs a cleanup of existing scan key data structures and then copies new scan key information into the scan descriptor. This approach is more efficient than ending the scan and starting a completely new one, as it preserves allocated memory contexts and other scan infrastructure.

The function is relatively simple but critical for performance in scenarios where the same index needs to be scanned multiple times with different predicates, such as in nested loop joins or when query parameters change.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure representing the ongoing index scan
- `scankey`: Array of new ScanKey structures containing the search predicates
- `nscankeys`: Number of scan keys in the scankey array (currently unused in the implementation)
- `orderbys`: Array of ScanKey structures for ordering (not supported by GIN, parameter ignored)
- `norderbys`: Number of ordering keys (not supported by GIN, parameter ignored)

## Dependencies
- Functions called/Symbols referenced:
  - [ginFreeScanKeys](ginFreeScanKeys.md): Frees existing scan key data structures and associated memory
  - `memmove`: Copies new scan key data into the scan descriptor
- Called from (representative examples):
  - [ginhandler](ginhandler.md): Part of the index access method interface

## Notes and Other Information
- GIN indexes do not support ordered scans, so the `orderbys` and `norderbys` parameters are effectively ignored
- The function assumes that the new scan keys are compatible with the existing scan infrastructure
- After calling this function, the scan will need to be repositioned (typically by calling the appropriate scan positioning function)
- This function is part of the standard index access method interface and is called by the PostgreSQL executor when scan parameters need to be changed
- Memory allocated for scan keys is managed through the scan context and will be automatically cleaned up when the scan ends