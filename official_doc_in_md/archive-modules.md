Chapter 49. Archive Modules  
---  
[Prev](replication-origins.md "Chapter 48. Replication Progress Tracking") | [Up](server-programming.md "Part V. Server Programming")| Part V. Server Programming| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](archive-module-init.md "49.1. Initialization Functions")  
  
* * *

## Chapter 49. Archive Modules

**Table of Contents**

[49.1. Initialization Functions](archive-module-init.md)
[49.2. Archive Module Callbacks](archive-module-callbacks.md)
    

[49.2.1. Startup Callback](archive-module-callbacks.md#ARCHIVE-MODULE-STARTUP)
[49.2.2. Check Callback](archive-module-callbacks.md#ARCHIVE-MODULE-CHECK)
[49.2.3. Archive Callback](archive-module-callbacks.md#ARCHIVE-MODULE-ARCHIVE)
[49.2.4. Shutdown Callback](archive-module-callbacks.md#ARCHIVE-MODULE-SHUTDOWN)

PostgreSQL provides infrastructure to create custom modules for continuous archiving (see [Section 25.3](continuous-archiving.md "25.3. Continuous Archiving and Point-in-Time Recovery \(PITR\)")). While archiving via a shell command (i.e., [archive_command](runtime-config-wal.md#GUC-ARCHIVE-COMMAND)) is much simpler, a custom archive module will often be considerably more robust and performant. 

When a custom [archive_library](runtime-config-wal.md#GUC-ARCHIVE-LIBRARY) is configured, PostgreSQL will submit completed WAL files to the module, and the server will avoid recycling or removing these WAL files until the module indicates that the files were successfully archived. It is ultimately up to the module to decide what to do with each WAL file, but many recommendations are listed at [Section 25.3.1](continuous-archiving.md#BACKUP-ARCHIVING-WAL "25.3.1. Setting Up WAL Archiving"). 

Archiving modules must at least consist of an initialization function (see [Section 49.1](archive-module-init.md "49.1. Initialization Functions")) and the required callbacks (see [Section 49.2](archive-module-callbacks.md "49.2. Archive Module Callbacks")). However, archive modules are also permitted to do much more (e.g., declare GUCs and register background workers). 

The `contrib/basic_archive` module contains a working example, which demonstrates some useful techniques. 

* * *

[Prev](replication-origins.md "Chapter 48. Replication Progress Tracking") | [Up](server-programming.md "Part V. Server Programming")|  [Next](archive-module-init.md "49.1. Initialization Functions")  
---|---|---  
Chapter 48. Replication Progress Tracking | [Home](index.md "PostgreSQL 17.5 Documentation")|  49.1. Initialization Functions
