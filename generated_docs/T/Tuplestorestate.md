# Tuplestorestate

## Location
src/backend/utils/sort/tuplestore.c: 103 - 182

## Overview
Tuplestorestate is the private state structure that manages tuple storage operations in PostgreSQL, providing functionality to store, retrieve, and manage tuples both in memory and on disk with support for multiple read pointers and various operational modes.

## Definition
```c
struct Tuplestorestate
{
    TupStoreStatus status;          /* enumerated value as shown above */
    int         eflags;             /* capability flags (OR of pointers flags) */
    bool        backward;           /* store extra length words in file? */
    bool        interXact;          /* keep open through transactions? */
    bool        truncated;          /* tuplestore_trim has removed tuples? */
    int64       availMem;           /* remaining memory available, in bytes */
    int64       allowedMem;         /* total memory allowed, in bytes */
    int64       tuples;             /* number of tuples added */
    BufFile    *myfile;             /* underlying file, or NULL if none */
    MemoryContext context;          /* memory context for holding tuples */
    ResourceOwner resowner;         /* resowner for holding temp files */

    /* Function pointers for tuple operations */
    void       *(*copytup) (Tuplestorestate *state, void *tup);
    void        (*writetup) (Tuplestorestate *state, void *tup);
    void       *(*readtup) (Tuplestorestate *state, unsigned int len);

    /* In-memory tuple storage */
    void      **memtuples;          /* array of pointers to pallocd tuples __pycache__/ any-script-mcp-repo/ any-script-mcp/ checkpointing_documentation/ contrib/ data/ generated_docs/ log/ misc/ official_doc_in_md/ output/ prompts/ scripts/ src/ topic_specific_generated_docs/