# FinalizeIncrementalManifest

## Location
src/backend/backup/basebackup_incremental.c: 229 - 264

## Overview
Completes the processing of incremental backup manifest data by parsing the final buffer chunk and cleaning up associated memory resources.

## Definition


## Detailed Description
This function is called after all manifest data chunks have been processed via AppendIncrementalManifestData to complete the incremental backup manifest parsing. It performs the final JSON parsing of any remaining data in the buffer, then cleans up all associated memory structures including the buffer data and the incremental parser state.

The function serves as the cleanup and finalization step in the incremental manifest processing pipeline, ensuring that:
- Any remaining buffered data is parsed with the final flag set to true
- All allocated memory for the buffer is released
- The incremental parser state is properly shut down
- Memory context management is handled correctly

## Parameters / Member Variables
- : IncrementalBackupInfo structure containing the incremental backup state, buffer, and parser state to finalize

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - json_parse_manifest_incremental_chunk
  - pfree
  - json_parse_manifest_incremental_shutdown
- Types referenced:
  - IncrementalBackupInfo
- Called from:
  - UploadManifest (src/backend/replication/walsender.c:715)

## Notes and Other Information
- This function must be called after all AppendIncrementalManifestData calls are complete
- The final json_parse_manifest_incremental_chunk call uses the 'final' flag set to true, indicating end-of-data
- Memory cleanup is thorough, releasing both buffer data and parser state resources
- Proper memory context switching ensures cleanup occurs in the correct memory context
- After this function completes, the IncrementalBackupInfo structure should not be used for further manifest data processing