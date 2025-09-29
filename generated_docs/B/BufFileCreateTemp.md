# BufFileCreateTemp

## Location
[src/backend/storage/file/buffile.c:193-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L193-L221)

## Overview
Creates a new BufFile for temporary file operations that can automatically expand to multiple physical files as data grows beyond file size limits.

## Definition
```c
BufFile *BufFileCreateTemp(bool interXact)
```

## Detailed Description
BufFileCreateTemp is the main public interface for creating temporary BufFiles in PostgreSQL. It creates a temporary file that can automatically expand to multiple physical files when the data exceeds MAX_PHYSICAL_FILESIZE bytes. The function ensures proper tablespace setup before creating the underlying temporary file and configures the BufFile with the appropriate transaction persistence settings.

The function first calls PrepareTempTablespaces() to ensure temporary tablespaces are properly configured, preventing hard-to-detect bugs where temp files would always be placed in the default tablespace. It then creates the underlying temporary file and wraps it in a BufFile structure using makeBufFile.

## Parameters / Member Variables
- `interXact`: Boolean flag indicating whether the temporary file should persist beyond the current transaction boundary

## Dependencies
- Functions called/Symbols referenced:
  - [PrepareTempTablespaces](../P/PrepareTempTablespaces.md) (ensures temp tablespace setup)
  - [OpenTemporaryFile](../O/OpenTemporaryFile.md) (creates the underlying temporary file)
  - [makeBufFile](../m/makeBufFile.md) (wraps the File in a BufFile structure)
- Called from (representative examples):
  - [gistInitBuildBuffers](../g/gistInitBuildBuffers.md) (GiST index building)
  - [InitializeBackupManifest](../I/InitializeBackupManifest.md) (backup operations)
  - [ExecHashJoinSaveTuple](../E/ExecHashJoinSaveTuple.md) (hash join execution)
  - [LogicalTapeSetCreate](../L/LogicalTapeSetCreate.md) (sort operations)
  - [tuplestore_puttuple_common](../t/tuplestore_puttuple_common.md) (tuple storage)

## Notes and Other Information
- This is a public function exported in buffile.h for use by other PostgreSQL modules
- The function automatically handles tablespace preparation, making it safer than lower-level alternatives
- Files created with interXact=true will survive transaction boundaries and must be explicitly cleaned up
- When interXact=true, the caller must ensure they are in a memory context and resource owner that survives transaction boundaries
- The BufFile can automatically expand to multiple physical files when data exceeds size limits
- Uses Assert to verify that file creation succeeded
- The underlying temporary files are managed by PostgreSQL's file management system

## Simplified Source

```c
// Create a new temporary BufFile
BufFile *BufFileCreateTemp(bool interXact)
{
    BufFile *file;
    File pfile;

    // Ensure temp tablespaces are set up properly
    // This prevents hard-to-detect bugs where temp files always
    // go to default tablespace
    PrepareTempTablespaces();

    // Create the underlying temporary file
    pfile = OpenTemporaryFile(interXact);
    Assert(pfile >= 0);

    // Wrap the File in a BufFile structure
    file = makeBufFile(pfile);
    file->isInterXact = interXact;

    return file;
}
```