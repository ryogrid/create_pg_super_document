# SummarizerReadLocalXLogPrivate

## Location
[src/backend/postmaster/walsummarizer.c:104-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L104-L124)

## Overview
SummarizerReadLocalXLogPrivate is a private data structure used as context for the xlogreader's page read callback function in the WAL summarizer process.

## Definition
```c
typedef struct
{
    TimeLineID  tli;
    bool        historic;
    XLogRecPtr  read_upto;
    bool        end_of_wal;
} SummarizerReadLocalXLogPrivate;
```

## Detailed Description
SummarizerReadLocalXLogPrivate serves as a context structure that carries state information needed by the WAL page read callback function (summarizer_read_local_xlog_page). This structure encapsulates the parameters and state required for reading WAL pages during the summarization process, including timeline information, read boundaries, and status flags that control the reading behavior.

## Parameters / Member Variables
- `tli`: Timeline ID indicating which timeline the WAL pages should be read from
- `historic`: Boolean flag indicating whether the pages being read are from a historic timeline (not the current one)
- `read_upto`: XLogRecPtr specifying the maximum LSN up to which WAL should be read
- `end_of_wal`: Boolean flag indicating whether the end of available WAL has been reached

## Dependencies
- Functions called/Symbols referenced:
  - [WalSummarizerData](../W/WalSummarizerData.md) (referenced in the same file context)
- Called from:
  - [SummarizeWAL](SummarizeWAL.md) (creates and uses instances of this structure)
  - [summarizer_read_local_xlog_page](../s/summarizer_read_local_xlog_page.md) (receives this structure as callback data)

## Notes and Other Information
- This structure is specifically designed as private data for XLogReader callback functions
- The structure enables the callback function to maintain context about reading boundaries and timeline information
- Used in conjunction with XLogReader infrastructure to control WAL page reading during summarization
- The end_of_wal flag helps manage the reading process when approaching the current end of the WAL stream
- Timeline information is crucial for handling WAL reading across timeline switches in PostgreSQL