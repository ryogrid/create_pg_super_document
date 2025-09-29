# StandbyTransactionIdIsPrepared

## Location
[src/backend/access/transam/twophase.c:1459-1486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1459-L1486)

## Overview
StandbyTransactionIdIsPrepared verifies whether a specific transaction ID corresponds to a prepared transaction during database recovery operations.

## Definition
bool StandbyTransactionIdIsPrepared(TransactionId xid)

## Detailed Description
This function is specifically designed for use during recovery mode to confirm if a given transaction ID (xid) represents a prepared transaction that exists in the two-phase commit system. It performs validation by reading the two-phase commit state file from disk and verifying that the transaction ID in the file header matches the requested transaction ID. The function returns false if two-phase commit is disabled (max_prepared_xacts <= 0) or if no corresponding prepared transaction file exists.

## Parameters / Member Variables
- `xid`: The transaction ID to check for prepared status. Must be a valid transaction ID.

## Dependencies
- Functions called/Symbols referenced:
  - [ReadTwoPhaseFile](../R/ReadTwoPhaseFile.md)
  - TransactionIdEquals
  - TwoPhaseFileHeader
- Called from (representative examples):
  - [KnownAssignedXidsRemovePreceding](../K/KnownAssignedXidsRemovePreceding.md)
  - [StandbyReleaseOldLocks](StandbyReleaseOldLocks.md)

## Notes and Other Information
- This function is specifically used during recovery operations, not normal runtime
- Returns false immediately if two-phase commit is disabled (max_prepared_xacts <= 0)
- Uses ReadTwoPhaseFile with the 'missing_ok' parameter set to true, allowing graceful handling of non-existent files
- Memory allocated for the file buffer is properly freed after validation

## Simplified Source

```c
bool
StandbyTransactionIdIsPrepared(TransactionId xid)
{
	char *buf;
	TwoPhaseFileHeader *hdr;
	bool result;

	Assert(TransactionIdIsValid(xid));

	// Early exit if two-phase commit disabled
	if (max_prepared_xacts <= 0)
		return false;

	// Read and validate two-phase file
	buf = ReadTwoPhaseFile(xid, true);
	if (buf == NULL)
		return false;

	// Check if transaction ID matches
	hdr = (TwoPhaseFileHeader *) buf;
	result = TransactionIdEquals(hdr->xid, xid);
	pfree(buf);

	return result;
}
```