# ReadDir

## Location
[src/backend/storage/file/fd.c:2906-2920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2906-L2920)

## Overview
ReadDir provides a simplified interface for reading directory entries with automatic error handling, wrapping the more complex ReadDirExtended function.

## Definition

```c
struct dirent *
ReadDir(DIR *dir, const char *dirname)
```
## Detailed Description
ReadDir is a convenience wrapper around ReadDirExtended that simplifies directory reading operations by automatically handling error conditions with ERROR-level reporting. It eliminates the need for tedious errno manipulation that would be required with raw readdir() calls. The function is designed to work seamlessly with AllocateDir, allowing for clean directory traversal patterns where a NULL dir parameter (indicating AllocateDir failure) is gracefully handled.

The function serves as the standard interface for directory reading throughout the PostgreSQL codebase, providing consistent error handling behavior across all directory operations. When an error occurs, it reports with ERROR level, which will cause the current transaction to abort.

## Parameters / Member Variables
- 0					      join_cur_level
5					      join_rel_level[join_cur_level]
=					      join_rel_level[level]
CODE_TREE.md				      key_symbols.txt
CODE_TREE_CONTRIB.md			      lastPHId
CurrentCmdInvalidMsgs			      lastnopr
ENTRY_POINTS.md				      lastpost
FuzzyAttrMatchState_documentation.md	      line_buf
LICENSE					      locktag_field2
MAX_SIMPLE_CHR				      locktag_lockmethodid
PMChildFlags[]				      log
PagePrecedes				      lsn[]
Pfdebug					      maxParallelHazard
R					      maxdepth
README.md				      min_join_parameterization_doc.md
SharedPromoteIsTriggered		      misc
T2					      mismatch_case.log
W					      nbanks
__pycache__				      nentries
accept_writes				      nitems
activeptr				      nwords
agginfos				      official_doc_in_md
any-script-mcp				      outBuffer
any-script-mcp-repo			      outMsgEnd
appendBinaryPQExpBuffer_doc.md		      outbufsize
appendPQExpBufferChar_doc.md		      output
appendPQExpBufferStr_doc.md		      p_next_resno
append_rel_list				      parallelModeNeeded
architecture_map.json			      parent
area					      parent_relid
attnums					      parse
auth					      pending_srf_tuples
b					      pgstat_clip_activity_doc.md
base.nKeys				      pgstat_info-
baserestrictcost			      pi_state.resultslot
blockState				      printed_subplans
build_child_join_rel_doc.md		      processed_tlist
build_join_rel_doc.md			      prompts
build_joinrel_restrictlist_doc.md	      python_version
build_joinrel_tlist_doc.md		      query_pathkeys
canon_pathkeys				      raw_fields[]
check_agg_arguments_context_documentation.md  rd_idattr
checkpointing_documentation		      rd_pubdesc
client_finished_auth			      recoveryWakeupLatch
cmd_queue_recycle			      remoterel.attnames[i]
compiled				      remoterel.natts
contrib					      reprocess_structs.txt
curTransactionContext			      requirements.txt
curaggcontext				      resnull
d.arraycoerce.amstate			      resvalue
d.arraycoerce.elemexprstate		      ri_ChildToRootMap
d.arraycoerce.resultelemtype		      ri_ReturningSlot
data					      ri_TrigNewSlot
dest_dboid				      ri_TrigOldSlot
ec_merging_done				      rows
enc					      rs_ctup.t_data
end_xact				      saved_errno
envvar					      scripts
es_query_cxt				      search
estimate				      setop_pathkeys
fast_forward				      smgr_cached_nblocks[forknum]
framehead_slot				      src
frameheadpos				      src_dboid
frametail_slot				      state
frametailpos				      strategy
functions				      subdfas[t-
generated_docs				      syncrep_method
glob-					      temp_slot_2
global_symbols.db			      topic_specific_generated_docs
gss					      trans-
gss-					      tuples
id]					      tuples_deleted
ii_CheckedUnchanged			      type
inh					      update_colnos
initial_rels				      update_struct_members.log
innermost_casenull			      weight
innermost_caseval			      write_location: Directory stream pointer returned by AllocateDir, or NULL if AllocateDir failed
- : Directory path name used only for error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - [ReadDirExtended](ReadDirExtended.md)
  - [DIR](../D/DIR.md) (system type)
  - [dirent](../d/dirent.md) (system structure)
- Called from (representative examples):
  - [CheckPointLogicalRewriteHeap](../C/CheckPointLogicalRewriteHeap.md)
  - [SlruScanDirectory](../S/SlruScanDirectory.md)  
  - [restoreTwoPhaseData](../r/restoreTwoPhaseData.md)
  - [XLogGetOldestSegno](../X/XLogGetOldestSegno.md)
  - [RemoveTempXlogFiles](RemoveTempXlogFiles.md)
  - [perform_base_backup](../p/perform_base_backup.md)
  - [sendDir](../s/sendDir.md)
  - [copydir](../c/copydir.md)
  - [ResetUnloggedRelations](ResetUnloggedRelations.md)
  - [db_dir_size](../d/db_dir_size.md)
  - [pg_ls_dir](../p/pg_ls_dir.md)

## Notes and Other Information
- Returns NULL when reaching end of directory or on error
- Automatically handles NULL dir parameter as indication of AllocateDir failure
- Uses ERROR level for all error reporting, causing transaction abort
- Part of PostgreSQL's file descriptor management subsystem
- Commonly used in directory traversal patterns with AllocateDir and FreeDir
- The dirname parameter is only used for error messages and must match the path used in AllocateDir
- Errno should not be modified between AllocateDir and ReadDir when using the NULL dir shortcut pattern