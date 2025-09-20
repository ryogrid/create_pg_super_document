# RecoveryPauseState

## Location
[src/include/access/xlogrecovery.h:49-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogrecovery.h#L49-L131)

## Overview
An enumeration that defines the different states of recovery pause functionality in PostgreSQL's Write-Ahead Log (WAL) recovery process.

## Definition

```c
typedef struct
{
	/*
	 * Information about the last valid or applied record, after which new WAL
	 * can be appended.  'lastRec' is the position where the last record
	 * starts, and 'endOfLog' is its end.  'lastPage' is a copy of the last
	 * partial page that contains endOfLog (or NULL if endOfLog is exactly at
	 * page boundary).  'lastPageBeginPtr' is the position where the last page
	 * begins.
	 *
	 * endOfLogTLI is the TLI in the filename of the XLOG segment containing
	 * the last applied record.  It could be different from lastRecTLI, if
	 * there was a timeline switch in that segment, and we were reading the
	 * old WAL from a segment belonging to a higher timeline.
	 */
	XLogRecPtr	lastRec;		/* start of last valid or applied record */
	TimeLineID	lastRecTLI;
	XLogRecPtr	endOfLog;		/* end of last valid or applied record */
	TimeLineID	endOfLogTLI;

	XLogRecPtr	lastPageBeginPtr;	/* LSN of page that contains endOfLog */
	char	   *lastPage;		/* copy of the last page, up to endOfLog */

	/*
	 * abortedRecPtr is the start pointer of a broken record at end of WAL
	 * when recovery completes; missingContrecPtr is the location of the first
	 * contrecord that went missing.  See CreateOverwriteContrecordRecord for
	 * details.
	 */
	XLogRecPtr	abortedRecPtr;
	XLogRecPtr	missingContrecPtr;

	/* short human-readable string describing why recovery ended */
	char	   *recoveryStopReason;

	/*
	 * If standby or recovery signal file was found, these flags are set
	 * accordingly.
	 */
	bool		standby_signal_file_found;
	bool		recovery_signal_file_found;
} EndOfWalRecoveryInfo;
```
## Detailed Description
The RecoveryPauseState enum represents the three possible states during WAL recovery pause operations. This mechanism allows administrators to temporarily halt recovery processing for maintenance, debugging, or other operational purposes. The enum provides a state machine with three distinct phases: normal operation, pause transition, and paused state.

Recovery pause is controlled through PostgreSQL's recovery control system and is used internally to manage the transition between active recovery and paused states safely with proper synchronization.

## Parameters / Member Variables
- : Normal recovery state where WAL replay continues without interruption
- : Intermediate state indicating a pause has been requested but recovery hasn't yet reached the paused state
- : Recovery is fully paused and WAL replay is halted

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type with no function calls)
- Called from (representative examples):
  - [GetRecoveryPauseState](../G/GetRecoveryPauseState.md) (src/backend/access/transam/xlogrecovery.c:3072)
  - [XLogRecoveryCtlData](../X/XLogRecoveryCtlData.md).recoveryPauseState (src/backend/access/transam/xlogrecovery.c:358)
  - SetRecoveryPauseState functions
  - Recovery pause management functions in xlogfuncs.c

## Notes and Other Information
- Used as a member variable in XLogRecoveryCtlData structure for tracking global recovery pause state
- State transitions are protected by spinlocks to ensure thread-safe access
- The intermediate RECOVERY_PAUSE_REQUESTED state allows for graceful pause transitions
- Used in conjunction with condition variables (recoveryNotPausedCV) for proper synchronization during pause/resume operations
- State management is primarily handled in src/backend/access/transam/xlogrecovery.c