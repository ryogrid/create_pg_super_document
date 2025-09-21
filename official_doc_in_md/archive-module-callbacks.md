49.2. Archive Module Callbacks  
---  
[Prev](archive-module-init.md "49.1. Initialization Functions") | [Up](archive-modules.md "Chapter 49. Archive Modules")| Chapter 49. Archive Modules| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](reference.md "Part VI. Reference")  
  
* * *

## 49.2. Archive Module Callbacks #

[49.2.1. Startup Callback](archive-module-callbacks.md#ARCHIVE-MODULE-STARTUP)
[49.2.2. Check Callback](archive-module-callbacks.md#ARCHIVE-MODULE-CHECK)
[49.2.3. Archive Callback](archive-module-callbacks.md#ARCHIVE-MODULE-ARCHIVE)
[49.2.4. Shutdown Callback](archive-module-callbacks.md#ARCHIVE-MODULE-SHUTDOWN)

The archive callbacks define the actual archiving behavior of the module. The server will call them as required to process each individual WAL file. 

### 49.2.1. Startup Callback #

The `startup_cb` callback is called shortly after the module is loaded. This callback can be used to perform any additional initialization required. If the archive module has any state, it can use `state->private_data` to store it. 
    
    
    typedef void (*ArchiveStartupCB) (ArchiveModuleState *state);
    

### 49.2.2. Check Callback #

The `check_configured_cb` callback is called to determine whether the module is fully configured and ready to accept WAL files (e.g., its configuration parameters are set to valid values). If no `check_configured_cb` is defined, the server always assumes the module is configured. 
    
    
    typedef bool (*ArchiveCheckConfiguredCB) (ArchiveModuleState *state);
    

If `true` is returned, the server will proceed with archiving the file by calling the `archive_file_cb` callback. If `false` is returned, archiving will not proceed, and the archiver will emit the following message to the server log: 
    
    
    WARNING:  archive_mode enabled, yet archiving is not configured
    

In the latter case, the server will periodically call this function, and archiving will proceed only when it returns `true`. 

### Note

When returning `false`, it may be useful to append some additional information to the generic warning message. To do that, provide a message to the `arch_module_check_errdetail` macro before returning `false`. Like `errdetail()`, this macro accepts a format string followed by an optional list of arguments. The resulting string will be emitted as the `DETAIL` line of the warning message. 

### 49.2.3. Archive Callback #

The `archive_file_cb` callback is called to archive a single WAL file. 
    
    
    typedef bool (*ArchiveFileCB) (ArchiveModuleState *state, const char *file, const char *path);
    

If `true` is returned, the server proceeds as if the file was successfully archived, which may include recycling or removing the original WAL file. If `false` is returned or an error is thrown, the server will keep the original WAL file and retry archiving later. _`file`_ will contain just the file name of the WAL file to archive, while _`path`_ contains the full path of the WAL file (including the file name). 

### Note

The `archive_file_cb` callback is called in a short-lived memory context that will be reset between invocations. If you need longer-lived storage, create a memory context in the module's `startup_cb` callback. 

### 49.2.4. Shutdown Callback #

The `shutdown_cb` callback is called when the archiver process exits (e.g., after an error) or the value of [archive_library](runtime-config-wal.md#GUC-ARCHIVE-LIBRARY) changes. If no `shutdown_cb` is defined, no special action is taken in these situations. If the archive module has any state, this callback should free it to avoid leaks. 
    
    
    typedef void (*ArchiveShutdownCB) (ArchiveModuleState *state);
    

* * *

[Prev](archive-module-init.md "49.1. Initialization Functions") | [Up](archive-modules.md "Chapter 49. Archive Modules")|  [Next](reference.md "Part VI. Reference")  
---|---|---  
49.1. Initialization Functions | [Home](index.md "PostgreSQL 17.5 Documentation")|  Part VI. Reference
