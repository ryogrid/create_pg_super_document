# BTReadPageState

## Location
[src/include/access/nbtree.h:1086-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L1086-L1117)

## Overview
BTReadPageState is a structure that maintains state information during B-tree page reading operations, specifically used across _bt_checkkeys calls for a single page during index scans.

## Definition

```c
typedef struct BTReadPageState
{
	/* Input parameters, set by _bt_readpage for _bt_checkkeys */
	ScanDirection dir;			/* current scan direction */
	OffsetNumber minoff;		/* Lowest non-pivot tuple's offset */
	OffsetNumber maxoff;		/* Highest non-pivot tuple's offset */
	IndexTuple	finaltup;		/* Needed by scans with array keys */
	BlockNumber prev_scan_page; /* previous _bt_parallel_release block */
	Page		page;			/* Page being read */

	/* Per-tuple input parameters, set by _bt_readpage for _bt_checkkeys */
	OffsetNumber offnum;		/* current tuple's page offset number */

	/* Output parameter, set by _bt_checkkeys for _bt_readpage */
	OffsetNumber skip;			/* Array keys "look ahead" skip offnum */
	bool		continuescan;	/* Terminate ongoing (primitive) index scan? */

	/*
	 * Input and output parameters, set and unset by both _bt_readpage and
	 * _bt_checkkeys to manage precheck optimizations
	 */
	bool		prechecked;		/* precheck set continuescan to 'true'? */
	bool		firstmatch;		/* at least one match so far?  */

	/*
	 * Private _bt_checkkeys state used to manage "look ahead" optimization
	 * (only used during scans with array keys)
	 */
	int16		rechecks;
	int16		targetdistance;

} BTReadPageState;
```
## Detailed Description
BTReadPageState serves as a communication mechanism between _bt_readpage and _bt_checkkeys functions during B-tree index scanning. It encapsulates all the necessary state information required to efficiently scan through tuples on a B-tree page, including optimization parameters for array key scans and look-ahead mechanisms. The structure is designed to minimize function call overhead by bundling related parameters and state variables together.

## Parameters / Member Variables
- 0					      join_rel_level[join_cur_level]
5					      join_rel_level[level]
=					      lastPHId
CODE_TREE.md				      lastnopr
CODE_TREE_CONTRIB.md			      lastpost
CurrentCmdInvalidMsgs			      line_buf
ENTRY_POINTS.md				      log
FuzzyAttrMatchState_documentation.md	      lsn[]
LICENSE					      maxParallelHazard
MAX_SIMPLE_CHR				      misc
Pfdebug					      mismatch_case.log
R					      nbanks
README.md				      nentries
SharedPromoteIsTriggered		      nitems
T2					      nwords
W					      official_doc_in_md
__pycache__				      outBuffer
accept_writes				      outMsgEnd
activeptr				      outbufsize
agginfos				      output
any-script-mcp				      p_next_resno
any-script-mcp-repo			      parallelModeNeeded
appendBinaryPQExpBuffer_doc.md		      parent
appendPQExpBufferChar_doc.md		      parent_relid
appendPQExpBufferStr_doc.md		      parse
append_rel_list				      pending_srf_tuples
area					      pgstat_clip_activity_doc.md
attnums					      pgstat_info-
auth					      pi_state.resultslot
b					      printed_subplans
base.nKeys				      processed_tlist
baserestrictcost			      prompts
blockState				      python_version
canon_pathkeys				      query_pathkeys
check_agg_arguments_context_documentation.md  raw_fields[]
cmd_queue_recycle			      rd_idattr
compiled				      rd_pubdesc
contrib					      recoveryWakeupLatch
curTransactionContext			      remoterel.attnames[i]
curaggcontext				      remoterel.natts
d.arraycoerce.amstate			      reprocess_structs.txt
d.arraycoerce.elemexprstate		      requirements.txt
d.arraycoerce.resultelemtype		      resnull
data					      resvalue
dest_dboid				      ri_ChildToRootMap
ec_merging_done				      ri_ReturningSlot
enc					      ri_TrigNewSlot
end_xact				      ri_TrigOldSlot
envvar					      rows
es_query_cxt				      rs_ctup.t_data
estimate				      saved_errno
fast_forward				      scripts
framehead_slot				      search
frameheadpos				      setop_pathkeys
frametail_slot				      src
frametailpos				      src_dboid
functions				      state
generated_docs				      strategy
glob-					      syncrep_method
global_symbols.db			      temp_slot_2
gss					      topic_specific_generated_docs
gss-					      trans-
ii_CheckedUnchanged			      tuples
inh					      tuples_deleted
initial_rels				      type
innermost_casenull			      update_colnos
innermost_caseval			      update_struct_members.log
join_cur_level				      write_location: Current scan direction (forward or backward)
- : Offset number of the lowest non-pivot tuple on the page
- : Offset number of the highest non-pivot tuple on the page
- : Index tuple needed specifically for scans involving array keys
- : Block number of the previous page released in parallel scans
- : Pointer to the actual page being scanned
- : Current tuple's offset number being processed
- : Output parameter indicating how many tuples to skip ahead for array key optimization
- : Boolean flag indicating whether the ongoing scan should continue
- : Flag indicating if precheck optimization set continuescan to true
- : Flag tracking whether at least one matching tuple has been found
- : Counter for managing look-ahead optimization in array key scans
- : Distance parameter used in look-ahead optimization logic

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirection (enum type)
  - OffsetNumber (type)
  - [IndexTuple](../I/IndexTuple.md) (type)
  - BlockNumber (type)
  - Page (type)
- Called from (representative examples):
  - [_bt_readpage](../b/_bt_readpage.md) (src/backend/access/nbtree/nbtsearch.c:1568)
  - [_bt_advance_array_keys](../b/_bt_advance_array_keys.md) (src/backend/access/nbtree/nbtutils.c:1789)
  - [_bt_checkkeys](../b/_bt_checkkeys.md) (src/backend/access/nbtree/nbtutils.c:3508)
  - [_bt_checkkeys_look_ahead](../b/_bt_checkkeys_look_ahead.md) (src/backend/access/nbtree/nbtutils.c:4072)

## Notes and Other Information
This structure is particularly important for array key scans where look-ahead optimizations can significantly improve performance. The separation between input, output, and bidirectional parameters reflects the careful design to support complex scanning scenarios while maintaining clean interfaces between the page reading and key checking phases of B-tree traversal.