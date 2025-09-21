49.1. Initialization Functions  
---  
[Prev](archive-modules.md "Chapter 49. Archive Modules") | [Up](archive-modules.md "Chapter 49. Archive Modules")| Chapter 49. Archive Modules| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](archive-module-callbacks.md "49.2. Archive Module Callbacks")  
  
* * *

## 49.1. Initialization Functions #

An archive library is loaded by dynamically loading a shared library with the [archive_library](runtime-config-wal.md#GUC-ARCHIVE-LIBRARY)'s name as the library base name. The normal library search path is used to locate the library. To provide the required archive module callbacks and to indicate that the library is actually an archive module, it needs to provide a function named `_PG_archive_module_init`. The result of the function must be a pointer to a struct of type `ArchiveModuleCallbacks`, which contains everything that the core code needs to know to make use of the archive module. The return value needs to be of server lifetime, which is typically achieved by defining it as a `static const` variable in global scope. 
    
    
    typedef struct ArchiveModuleCallbacks
    {
        ArchiveStartupCB startup_cb;
        ArchiveCheckConfiguredCB check_configured_cb;
        ArchiveFileCB archive_file_cb;
        ArchiveShutdownCB shutdown_cb;
    } ArchiveModuleCallbacks;
    typedef const ArchiveModuleCallbacks *(*ArchiveModuleInit) (void);
    

Only the `archive_file_cb` callback is required. The others are optional. 

* * *

[Prev](archive-modules.md "Chapter 49. Archive Modules") | [Up](archive-modules.md "Chapter 49. Archive Modules")|  [Next](archive-module-callbacks.md "49.2. Archive Module Callbacks")  
---|---|---  
Chapter 49. Archive Modules | [Home](index.md "PostgreSQL 17.5 Documentation")|  49.2. Archive Module Callbacks
