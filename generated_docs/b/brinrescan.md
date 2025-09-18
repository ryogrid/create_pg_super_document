# brinrescan

## Location
[src/backend/access/brin/brin.c:948-967](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L948-L967)

## Overview
Re-initializes state for a BRIN index scan by updating the scan keys if provided, allowing the scan to be restarted with new search conditions.

## Definition


## Detailed Description
The brinrescan function provides the ability to restart a BRIN index scan with potentially new scan keys. This is part of the standard PostgreSQL index access method interface. The function currently implements a simple approach where it copies new scan keys into the scan descriptor if provided.

The function includes a comment noting that other index access methods (like B-tree) perform scan key preprocessing at this point to optimize performance by removing redundant keys or detecting impossible conditions. This optimization could potentially be added to BRIN in the future.

Unlike some other index types, BRIN rescan is relatively lightweight since BRIN indexes don't maintain complex internal scan state that needs extensive reinitialization.

## Parameters / Member Variables
- : IndexScanDesc containing the current scan state to be reinitialized
- : Array of new scan keys to use for the rescan (can be NULL)
- : Number of scan keys in the scankey array
- : Array of order-by keys (not used in BRIN, can be NULL)  
- : Number of order-by keys (not used in BRIN)

## Dependencies
- Functions called/Symbols referenced:
  - memmove: Copies new scan keys into the scan descriptor
- Called from (representative examples):
  - [brinhandler](brinhandler.md): BRIN access method handler registration

## Notes and Other Information
- Order-by parameters are not used since BRIN indexes don't support ordered scans
- The function only copies scan keys if both scankey is non-NULL and scan->numberOfKeys > 0
- No preprocessing or optimization of scan keys is currently performed
- This is a standard index access method interface function required for all PostgreSQL index types
- Future optimizations could include scan key preprocessing similar to other index types like B-tree