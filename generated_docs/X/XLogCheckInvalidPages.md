# XLogCheckInvalidPages

## Location
[src/backend/access/transam/xlogutils.c:245-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L245-L313)

## Overview
Checks for and reports any remaining invalid page entries in the invalid page hash table, typically called during recovery consistency checking to ensure all WAL references to invalid pages have been resolved.

## Definition

```c
void
XLogCheckInvalidPages(void)
```
## Detailed Description
This function iterates through the global  hash table to identify any remaining invalid page entries that haven't been resolved during WAL recovery. It employs a two-phase reporting strategy: first emitting WARNING messages for all remaining invalid entries to provide comprehensive diagnostic information, then issuing either a WARNING or PANIC depending on the  setting.

The function serves as a final validation step in WAL recovery, ensuring that all page references in the WAL stream correspond to valid, accessible pages. If invalid pages remain, it indicates potential data corruption or incomplete recovery.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [report_invalid_page](../r/report_invalid_page.md)
  - [hash_destroy](../h/hash_destroy.md)
  - elog
- Data structures used:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [xl_invalid_page](../x/xl_invalid_page.md)
  - invalid_page_tab (global hash table)
- Called from:
  - [CheckRecoveryConsistency](../C/CheckRecoveryConsistency.md) (src/backend/access/transam/xlogrecovery.c:2235)

## Notes and Other Information
- The function uses a sequential scan approach to report all invalid pages before taking any fatal action
- Behavior is controlled by the  configuration parameter
- When  is true, invalid pages generate warnings instead of panic
- The invalid_page_tab hash table is destroyed and reset to NULL after processing
- This function is typically called near the end of recovery to ensure data consistency

## Simplified Source

```c
void
XLogCheckInvalidPages(void)
{
	HASH_SEQ_STATUS status;
	xl_invalid_page *hentry;
	bool foundone = false;

	if (invalid_page_tab == NULL)
		return;  // Nothing to check

	// Scan through all remaining invalid page entries
	hash_seq_init(&status, invalid_page_tab);

	while ((hentry = (xl_invalid_page *) hash_seq_search(&status)) != NULL)
	{
		// Report each invalid page with details
		report_invalid_page(WARNING, hentry->key.locator,
				   hentry->key.forkno, hentry->key.blkno, hentry->present);
		foundone = true;
	}

	// If any invalid pages found, log final message
	if (foundone)
		elog(ignore_invalid_pages ? WARNING : PANIC,
			 "WAL contains references to invalid pages");

	// Clean up the hash table
	hash_destroy(invalid_page_tab);
	invalid_page_tab = NULL;
}
```