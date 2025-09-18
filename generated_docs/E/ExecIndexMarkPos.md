# ExecIndexMarkPos

## Location
src/backend/executor/nodeIndexscan.c: 813 - 849

## Overview
Marks the current position in an index scan, allowing the scan to be restored to this position later for EvalPlanQual (EPQ) operations.

## Definition


## Detailed Description
The `ExecIndexMarkPos` function implements position marking functionality for index scans, which is essential for EvalPlanQual (EPQ) recheck operations in PostgreSQL's MVCC implementation. When concurrent transactions modify data that affects query results, PostgreSQL may need to recheck query conditions against updated tuple versions.

The function includes special handling for EPQ contexts. When operating within an EPQ recheck and a test tuple exists for the relation, the function avoids accessing the index directly since the recheck should use the test tuple instead. This prevents conflicts between normal index scanning and EPQ tuple substitution.

The function assumes that at least one tuple has been read before marking is attempted, ensuring that the index scan descriptor is properly initialized.

## Parameters
- `node`: Pointer to the IndexScanState containing the index scan state and descriptor to mark

## Dependencies
- Functions called/Symbols referenced:
  - [index_markpos](../i/index_markpos.md)
  - elog
- Data types used:
  - [IndexScanState](../I/IndexScanState.md)
  - [EState](EState.md)
  - [EPQState](EPQState.md)
  - Scan
  - Index

## Called From
- [ExecMarkPos](ExecMarkPos.md) (src/backend/executor/execAmi.c:331)

## Notes and Other Information
- Assumes at least one tuple has been read before marking (iss_ScanDesc is non-NULL)
- Includes special EPQ (EvalPlanQual) handling for concurrent transaction scenarios
- Verifies EPQ state consistency and prevents improper marking during EPQ rechecks
- Uses Assert to validate scan relation ID in debug builds
- Returns early without marking when EPQ test tuples exist for the relation
- Part of the broader position marking/restoration framework used in cursor operations
- Critical for implementing MVCC-compliant query execution in concurrent environments
- Works in conjunction with ExecIndexRestrPos to restore marked positions