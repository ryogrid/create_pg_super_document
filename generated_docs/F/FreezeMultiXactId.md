# FreezeMultiXactId

## Location
[src/backend/access/heap/heapam.c:6659-7008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L6659-L7008)

## Overview
A static function that determines how to handle MultiXactId values during tuple freezing, deciding whether to preserve, replace, or invalidate the MultiXactId based on vacuum cutoffs and the status of member transactions.

## Definition

```c
struct VacuumCutoffs *cutoffs, uint16 *flags,
				  HeapPageFreeze *pagefrz)
{
	TransactionId newxmax;
	MultiXactMember *members;
	int			nmembers;
	bool		need_replace;
	int			nnewmembers;
	MultiXactMember *newmembers;
	bool		has_lockers;
	TransactionId update_xid;
	bool		update_committed;
	TransactionId FreezePageRelfrozenXid;

	*flags = 0;

	/* We should only be called in Multis */
	Assert(t_infomask & HEAP_XMAX_IS_MULTI);

	if (!MultiXactIdIsValid(multi) ||
		HEAP_LOCKED_UPGRADED(t_infomask))
	{
		*flags |= FRM_INVALIDATE_XMAX;
		pagefrz->freeze_required = true;
		return InvalidTransactionId;
	}
	else if (MultiXactIdPrecedes(multi, cutoffs->relminmxid))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("found multixact %u from before relminmxid %u",
								 multi, cutoffs->relminmxid)));
	else if (MultiXactIdPrecedes(multi, cutoffs->OldestMxact))
	{
		TransactionId update_xact;

		/*
		 * This old multi cannot possibly have members still running, but
		 * verify just in case.  If it was a locker only, it can be removed
		 * without any further consideration; but if it contained an update,
		 * we might need to preserve it.
		 */
		if (MultiXactIdIsRunning(multi,
								 HEAP_XMAX_IS_LOCKED_ONLY(t_infomask)))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("multixact %u from before multi freeze cutoff %u found to be still running",
									 multi, cutoffs->OldestMxact)));

		if (HEAP_XMAX_IS_LOCKED_ONLY(t_infomask))
		{
			*flags |= FRM_INVALIDATE_XMAX;
			pagefrz->freeze_required = true;
			return InvalidTransactionId;
		}

		/* replace multi with single XID for its updater? */
		update_xact = MultiXactIdGetUpdateXid(multi, t_infomask);
		if (TransactionIdPrecedes(update_xact, cutoffs->relfrozenxid))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("multixact %u contains update XID %u from before relfrozenxid %u",
									 multi, update_xact,
									 cutoffs->relfrozenxid)));
		else if (TransactionIdPrecedes(update_xact, cutoffs->OldestXmin))
		{
			/*
			 * Updater XID has to have aborted (otherwise the tuple would have
			 * been pruned away instead, since updater XID is < OldestXmin).
			 * Just remove xmax.
			 */
			if (TransactionIdDidCommit(update_xact))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("multixact %u contains committed update XID %u from before removable cutoff %u",
										 multi, update_xact,
										 cutoffs->OldestXmin)));
			*flags |= FRM_INVALIDATE_XMAX;
			pagefrz->freeze_required = true;
			return InvalidTransactionId;
		}

		/* Have to keep updater XID as new xmax */
		*flags |= FRM_RETURN_IS_XID;
		pagefrz->freeze_required = true;
		return update_xact;
	}

	/*
	 * Some member(s) of this Multi may be below FreezeLimit xid cutoff, so we
	 * need to walk the whole members array to figure out what to do, if
	 * anything.
	 */
	nmembers =
		GetMultiXactIdMembers(multi, &members, false,
							  HEAP_XMAX_IS_LOCKED_ONLY(t_infomask));
	if (nmembers <= 0)
	{
		/* Nothing worth keeping */
		*flags |= FRM_INVALIDATE_XMAX;
		pagefrz->freeze_required = true;
		return InvalidTransactionId;
	}

	/*
	 * The FRM_NOOP case is the only case where we might need to ratchet back
	 * FreezePageRelfrozenXid or FreezePageRelminMxid.  It is also the only
	 * case where our caller might ratchet back its NoFreezePageRelfrozenXid
	 * or NoFreezePageRelminMxid "no freeze" trackers to deal with a multi.
	 * FRM_NOOP handling should result in the NewRelfrozenXid/NewRelminMxid
	 * trackers managed by VACUUM being ratcheting back by xmax to the degree
	 * required to make it safe to leave xmax undisturbed, independent of
	 * whether or not page freezing is triggered somewhere else.
	 *
	 * Our policy is to force freezing in every case other than FRM_NOOP,
	 * which obviates the need to maintain either set of trackers, anywhere.
	 * Every other case will reliably execute a freeze plan for xmax that
	 * either replaces xmax with an XID/MXID >= OldestXmin/OldestMxact, or
	 * sets xmax to an InvalidTransactionId XID, rendering xmax fully frozen.
	 * (VACUUM's NewRelfrozenXid/NewRelminMxid trackers are initialized with
	 * OldestXmin/OldestMxact, so later values never need to be tracked here.)
	 */
	need_replace = false;
	FreezePageRelfrozenXid = pagefrz->FreezePageRelfrozenXid;
	for (int i = 0; i < nmembers; i++)
	{
		TransactionId xid = members[i].xid;

		Assert(!TransactionIdPrecedes(xid, cutoffs->relfrozenxid));

		if (TransactionIdPrecedes(xid, cutoffs->FreezeLimit))
		{
			/* Can't violate the FreezeLimit postcondition */
			need_replace = true;
			break;
		}
		if (TransactionIdPrecedes(xid, FreezePageRelfrozenXid))
			FreezePageRelfrozenXid = xid;
	}

	/* Can't violate the MultiXactCutoff postcondition, either */
	if (!need_replace)
		need_replace = MultiXactIdPrecedes(multi, cutoffs->MultiXactCutoff);

	if (!need_replace)
	{
		/*
		 * vacuumlazy.c might ratchet back NewRelminMxid, NewRelfrozenXid, or
		 * both together to make it safe to retain this particular multi after
		 * freezing its page
		 */
		*flags |= FRM_NOOP;
		pagefrz->FreezePageRelfrozenXid = FreezePageRelfrozenXid;
		if (MultiXactIdPrecedes(multi, pagefrz->FreezePageRelminMxid))
			pagefrz->FreezePageRelminMxid = multi;
		pfree(members);
		return multi;
	}

	/*
	 * Do a more thorough second pass over the multi to figure out which
	 * member XIDs actually need to be kept.  Checking the precise status of
	 * individual members might even show that we don't need to keep anything.
	 * That is quite possible even though the Multi must be >= OldestMxact,
	 * since our second pass only keeps member XIDs when it's truly necessary;
	 * even member XIDs >= OldestXmin often won't be kept by second pass.
	 */
	nnewmembers = 0;
	newmembers = palloc(sizeof(MultiXactMember) * nmembers);
	has_lockers = false;
	update_xid = InvalidTransactionId;
	update_committed = false;

	/*
	 * Determine whether to keep each member xid, or to ignore it instead
	 */
	for (int i = 0; i < nmembers; i++)
	{
		TransactionId xid = members[i].xid;
		MultiXactStatus mstatus = members[i].status;

		Assert(!TransactionIdPrecedes(xid, cutoffs->relfrozenxid));

		if (!ISUPDATE_from_mxstatus(mstatus))
		{
			/*
			 * Locker XID (not updater XID).  We only keep lockers that are
			 * still running.
			 */
			if (TransactionIdIsCurrentTransactionId(xid) ||
				TransactionIdIsInProgress(xid))
			{
				if (TransactionIdPrecedes(xid, cutoffs->OldestXmin))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg_internal("multixact %u contains running locker XID %u from before removable cutoff %u",
											 multi, xid,
											 cutoffs->OldestXmin)));
				newmembers[nnewmembers++] = members[i];
				has_lockers = true;
			}

			continue;
		}

		/*
		 * Updater XID (not locker XID).  Should we keep it?
		 *
		 * Since the tuple wasn't totally removed when vacuum pruned, the
		 * update Xid cannot possibly be older than OldestXmin cutoff unless
		 * the updater XID aborted.  If the updater transaction is known
		 * aborted or crashed then it's okay to ignore it, otherwise not.
		 *
		 * In any case the Multi should never contain two updaters, whatever
		 * their individual commit status.  Check for that first, in passing.
		 */
		if (TransactionIdIsValid(update_xid))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("multixact %u has two or more updating members",
									 multi),
					 errdetail_internal("First updater XID=%u second updater XID=%u.",
										update_xid, xid)));

		/*
		 * As with all tuple visibility routines, it's critical to test
		 * TransactionIdIsInProgress before TransactionIdDidCommit, because of
		 * race conditions explained in detail in heapam_visibility.c.
		 */
		if (TransactionIdIsCurrentTransactionId(xid) ||
			TransactionIdIsInProgress(xid))
			update_xid = xid;
		else if (TransactionIdDidCommit(xid))
		{
			/*
			 * The transaction committed, so we can tell caller to set
			 * HEAP_XMAX_COMMITTED.  (We can only do this because we know the
			 * transaction is not running.)
			 */
			update_committed = true;
			update_xid = xid;
		}
		else
		{
			/*
			 * Not in progress, not committed -- must be aborted or crashed;
			 * we can ignore it.
			 */
			continue;
		}

		/*
		 * We determined that updater must be kept -- add it to pending new
		 * members list
		 */
		if (TransactionIdPrecedes(xid, cutoffs->OldestXmin))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("multixact %u contains committed update XID %u from before removable cutoff %u",
									 multi, xid, cutoffs->OldestXmin)));
		newmembers[nnewmembers++] = members[i];
	}

	pfree(members);

	/*
	 * Determine what to do with caller's multi based on information gathered
	 * during our second pass
	 */
	if (nnewmembers == 0)
	{
		/* Nothing worth keeping */
		*flags |= FRM_INVALIDATE_XMAX;
		newxmax = InvalidTransactionId;
	}
	else if (TransactionIdIsValid(update_xid) && !has_lockers)
	{
		/*
		 * If there's a single member and it's an update, pass it back alone
		 * without creating a new Multi.  (XXX we could do this when there's a
		 * single remaining locker, too, but that would complicate the API too
		 * much; moreover, the case with the single updater is more
		 * interesting, because those are longer-lived.)
		 */
		Assert(nnewmembers == 1);
		*flags |= FRM_RETURN_IS_XID;
		if (update_committed)
			*flags |= FRM_MARK_COMMITTED;
		newxmax = update_xid;
	}
	else
	{
		/*
		 * Create a new multixact with the surviving members of the previous
		 * one, to set as new Xmax in the tuple
		 */
		newxmax = MultiXactIdCreateFromMembers(nnewmembers, newmembers);
		*flags |= FRM_RETURN_IS_MULTI;
	}

	pfree(newmembers);

	pagefrz->freeze_required = true;
	return newxmax;
}

/*
 * heap_prepare_freeze_tuple
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * are older than the OldestXmin and/or OldestMxact freeze cutoffs.  If so,
 * setup enough state (in the *frz output argument) to enable caller to
 * process this tuple as part of freezing its page, and return true.  Return
 * false if nothing can be changed about the tuple right now.
 *
 * Also sets *totally_frozen to true if the tuple will be totally frozen once
 * caller executes returned freeze plan (or if the tuple was already totally
 * frozen by an earlier VACUUM).  This indicates that there are no remaining
 * XIDs or MultiXactIds that will need to be processed by a future VACUUM.
 *
 * VACUUM caller must assemble HeapTupleFreeze freeze plan entries for every
 * tuple that we returned true for, and then execute freezing.  Caller must
 * initialize pagefrz fields for page as a whole before first call here for
 * each heap page.
 *
 * VACUUM caller decides on whether or not to freeze the page as a whole.
 * We'll often prepare freeze plans for a page that caller just discards.
 * However, VACUUM doesn't always get to make a choice;
```
## Detailed Description
FreezeMultiXactId is a critical component of PostgreSQL's tuple freezing mechanism that specifically handles MultiXactId values in tuple headers. During VACUUM operations, this function analyzes a MultiXactId and its member transactions to determine the appropriate action based on various age-based cutoffs.

The function implements sophisticated logic to:
1. Validate the MultiXactId and check for corruption
2. Handle very old MultiXactIds that predate cutoff limits  
3. Analyze individual member transactions within the MultiXactId
4. Decide whether to preserve, replace with a single XID, create a new MultiXactId, or invalidate the field entirely
5. Ensure proper freezing postconditions are maintained

The decision-making process considers multiple factors including transaction commit status, whether transactions are still running, and various vacuum cutoff thresholds. The function helps prevent transaction ID wraparound while maintaining data integrity.

## Parameters
- : The MultiXactId value to be processed during freezing
- : Tuple header infomask bits providing context about the MultiXactId
- : Structure containing various vacuum cutoff thresholds (FreezeLimit, OldestXmin, etc.)
- : Output parameter indicating what action the caller should take (FRM_NOOP, FRM_INVALIDATE_XMAX, etc.)
- : Input/output structure for managing page-level freezing state

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md)
  - [MultiXactIdGetUpdateXid](../M/MultiXactIdGetUpdateXid.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
  - HEAP_XMAX_IS_MULTI
  - HEAP_LOCKED_UPGRADED
  - HEAP_XMAX_IS_LOCKED_ONLY
  - ISUPDATE_from_mxstatus
- Called from:
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md)

## Notes and Other Information
- **Return Value Interpretation**: The returned TransactionId's meaning depends on the flags set:
  - With FRM_RETURN_IS_XID: Single XID to use as new xmax
  - With FRM_RETURN_IS_MULTI: New MultiXactId to use as new xmax
  - With FRM_INVALIDATE_XMAX: Return value should be ignored, xmax gets invalidated
  - With FRM_NOOP: Return value is the original multi, no changes needed
- **Page-Level Freezing**: The function coordinates with the caller to manage page-level freezing requirements
- **SLRU Optimization**: Designed to minimize MultiXact member SLRU buffer misses through proactive processing
- **Corruption Detection**: Includes extensive validation and error reporting for data corruption scenarios
- **Member Transaction Handling**: Distinguishes between locker and updater transactions, keeping only necessary ones
- **Vacuum Integration**: Works closely with vacuum cutoff management to ensure safe transaction ID advancement
- **Critical for MVCC**: Essential for maintaining PostgreSQL's MVCC (Multi-Version Concurrency Control) semantics during freezing