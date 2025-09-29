# ParseCommitRecord

## Location
[src/backend/access/rmgrdesc/xactdesc.c:35-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/xactdesc.c#L35-L140)

## Overview
ParseCommitRecord parses the WAL (Write-Ahead Log) format of a transaction commit record and converts it into an easier-to-understand structured format for use by both backend and frontend code.

## Definition

```c
void
ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, xl_xact_parsed_commit *parsed)
```
## Detailed Description
This function is responsible for parsing binary WAL commit record data into a structured format that can be easily processed by various PostgreSQL components. It extracts transaction metadata including timing information, database/tablespace identifiers, subtransaction information, relation file locators, dropped statistics, invalidation messages, two-phase commit data, and replication origin details. The function handles variable-length records by sequentially parsing optional sections based on info flags, making it suitable for both WAL replay in the backend and WAL analysis in frontend tools like pg_waldump.

## Parameters / Member Variables
- : Info flags indicating which optional sections are present in the WAL record
- : Pointer to the raw WAL commit record data structure
- : Output structure to store the parsed commit record information

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - [strlcpy](../s/strlcpy.md)
  - memcpy
  - MinSizeOfXactCommit
  - XLOG_XACT_HAS_INFO
  - XACT_XINFO_HAS_DBINFO
  - XACT_XINFO_HAS_SUBXACTS
  - XACT_XINFO_HAS_RELFILELOCATORS
  - XACT_XINFO_HAS_DROPPED_STATS
  - XACT_XINFO_HAS_INVALS
  - XACT_XINFO_HAS_TWOPHASE
  - XACT_XINFO_HAS_GID
  - XACT_XINFO_HAS_ORIGIN
- Called from (representative examples):
  - [xact_desc_commit](../x/xact_desc_commit.md)
  - [xact_redo](../x/xact_redo.md)
  - [recoveryStopsBefore](../r/recoveryStopsBefore.md)
  - [recoveryStopsAfter](../r/recoveryStopsAfter.md)
  - [SummarizeXactRecord](../S/SummarizeXactRecord.md)
  - [xact_decode](../x/xact_decode.md)

## Notes and Other Information
- Located in xactdesc.c to allow sharing between backend and frontend code
- Handles variable-length WAL records with optional sections
- No alignment is guaranteed after the XACT_XINFO_HAS_TWOPHASE section
- The function initializes the parsed structure and progressively fills it based on available data sections
- Critical for WAL replay, recovery, replication, and WAL analysis tools

## Simplified Source

```c
void
ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, xl_xact_parsed_commit *parsed)
{
	char *data = ((char *) xlrec) + MinSizeOfXactCommit;

	memset(parsed, 0, sizeof(*parsed));
	parsed->xinfo = 0;  // Default if no XLOG_XACT_HAS_INFO
	parsed->xact_time = xlrec->xact_time;

	// Parse extended info if present
	if (info & XLOG_XACT_HAS_INFO)
	{
		xl_xact_xinfo *xl_xinfo = (xl_xact_xinfo *) data;
		parsed->xinfo = xl_xinfo->xinfo;
		data += sizeof(xl_xact_xinfo);
	}

	// Parse database info
	if (parsed->xinfo & XACT_XINFO_HAS_DBINFO)
	{
		xl_xact_dbinfo *xl_dbinfo = (xl_xact_dbinfo *) data;
		parsed->dbId = xl_dbinfo->dbId;
		parsed->tsId = xl_dbinfo->tsId;
		data += sizeof(xl_xact_dbinfo);
	}

	// Parse subtransaction info
	if (parsed->xinfo & XACT_XINFO_HAS_SUBXACTS)
	{
		xl_xact_subxacts *xl_subxacts = (xl_xact_subxacts *) data;
		parsed->nsubxacts = xl_subxacts->nsubxacts;
		parsed->subxacts = xl_subxacts->subxacts;
		data += MinSizeOfXactSubxacts + parsed->nsubxacts * sizeof(TransactionId);
	}

	// Parse relation file locators
	if (parsed->xinfo & XACT_XINFO_HAS_RELFILELOCATORS)
	{
		xl_xact_relfilelocators *xl_rellocators = (xl_xact_relfilelocators *) data;
		parsed->nrels = xl_rellocators->nrels;
		parsed->xlocators = xl_rellocators->xlocators;
		data += MinSizeOfXactRelfileLocators + xl_rellocators->nrels * sizeof(RelFileLocator);
	}

	// Parse dropped statistics
	if (parsed->xinfo & XACT_XINFO_HAS_DROPPED_STATS)
	{
		xl_xact_stats_items *xl_drops = (xl_xact_stats_items *) data;
		parsed->nstats = xl_drops->nitems;
		parsed->stats = xl_drops->items;
		data += MinSizeOfXactStatsItems + xl_drops->nitems * sizeof(xl_xact_stats_item);
	}

	// Parse invalidation messages (commit only)
	if (parsed->xinfo & XACT_XINFO_HAS_INVALS)
	{
		xl_xact_invals *xl_invals = (xl_xact_invals *) data;
		parsed->nmsgs = xl_invals->nmsgs;
		parsed->msgs = xl_invals->msgs;
		data += MinSizeOfXactInvals + xl_invals->nmsgs * sizeof(SharedInvalidationMessage);
	}

	// Parse two-phase info
	if (parsed->xinfo & XACT_XINFO_HAS_TWOPHASE)
	{
		xl_xact_twophase *xl_twophase = (xl_xact_twophase *) data;
		parsed->twophase_xid = xl_twophase->xid;
		data += sizeof(xl_xact_twophase);

		if (parsed->xinfo & XACT_XINFO_HAS_GID)
		{
			strlcpy(parsed->twophase_gid, data, sizeof(parsed->twophase_gid));
			data += strlen(data) + 1;
		}
	}

	// Parse origin info (no alignment guaranteed)
	if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
	{
		xl_xact_origin xl_origin;
		memcpy(&xl_origin, data, sizeof(xl_origin));
		parsed->origin_lsn = xl_origin.origin_lsn;
		parsed->origin_timestamp = xl_origin.origin_timestamp;
		data += sizeof(xl_xact_origin);
	}
}
```