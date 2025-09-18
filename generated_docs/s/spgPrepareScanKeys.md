# spgPrepareScanKeys

## Location
src/backend/access/spgist/spgscan.c: 208 - 303

## Overview
Prepares scan keys from caller-given scan keys in the SpGistScanOpaque structure, processing null-related conditions and eliminating null considerations for opclass consistent functions.

## Definition
```c
static void spgPrepareScanKeys(IndexScanDesc scan)
```

## Detailed Description
This static function processes the scan keys provided by the caller and prepares them for use in SP-GiST index scanning. It handles null-related logic by separating IS NULL and IS NOT NULL conditions from regular scan conditions, setting appropriate flags (searchNulls, searchNonNulls) to control which parts of the index need to be scanned.

The function assumes all SPGiST-indexable operators are strict, meaning any null RHS value makes the scan condition unsatisfiable. It processes order-by clauses by removing NULL keys while maintaining offset mappings. For regular scan keys, it filters out null arguments and separates null-specific conditions (IS NULL/IS NOT NULL) from ordinary qualifiers.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan descriptor with keys and order-by clauses to process

## Dependencies
- Functions called/Symbols referenced:
  - SpGistScanOpaque (type)
  - [IndexScanDesc](../I/IndexScanDesc.md) (type)
  - ScanKey (type)
  - SK_ISNULL (flag)
  - SK_SEARCHNULL (flag)
  - SK_SEARCHNOTNULL (flag)
- Called from:
  - [spgrescan](spgrescan.md) (src/backend/access/spgist/spgscan.c:419)

## Notes and Other Information
- This is a static function internal to the spgscan.c module
- Sets searchNulls, searchNonNulls, numberOfKeys, and keyData fields of the SpGistScanOpaque structure
- Handles the logic that IS NULL combined with other conditions is unsatisfiable
- Processes order-by data by removing NULL keys and maintaining offset mappings for non-NULL keys
- Assumes SPGiST operators are strict (null inputs produce null outputs)
- If no qualifiers are provided, enables both null and non-null searches for a whole-index scan