Chapter 61. Table Access Method Interface Definition  
---  
[Prev](geqo-biblio.md "60.4. Further Reading") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](indexam.md "Chapter 62. Index Access Method Interface Definition")  
  
* * *

## Chapter 61. Table Access Method Interface Definition

This chapter explains the interface between the core PostgreSQL system and _table access methods_ , which manage the storage for tables. The core system knows little about these access methods beyond what is specified here, so it is possible to develop entirely new access method types by writing add-on code. 

Each table access method is described by a row in the [`pg_am`](catalog-pg-am.md "51.3. pg_am") system catalog. The `pg_am` entry specifies a name and a _handler function_ for the table access method. These entries can be created and deleted using the [CREATE ACCESS METHOD](sql-create-access-method.md "CREATE ACCESS METHOD") and [DROP ACCESS METHOD](sql-drop-access-method.md "DROP ACCESS METHOD") SQL commands. 

A table access method handler function must be declared to accept a single argument of type `internal` and to return the pseudo-type `table_am_handler`. The argument is a dummy value that simply serves to prevent handler functions from being called directly from SQL commands. The result of the function must be a pointer to a struct of type `TableAmRoutine`, which contains everything that the core code needs to know to make use of the table access method. The return value needs to be of server lifetime, which is typically achieved by defining it as a `static const` variable in global scope. The `TableAmRoutine` struct, also called the access method's _API struct_ , defines the behavior of the access method using callbacks. These callbacks are pointers to plain C functions and are not visible or callable at the SQL level. All the callbacks and their behavior is defined in the `TableAmRoutine` structure (with comments inside the struct defining the requirements for callbacks). Most callbacks have wrapper functions, which are documented from the point of view of a user (rather than an implementor) of the table access method. For details, please refer to the [ `src/include/access/tableam.h`](https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/include/access/tableam.h;hb=HEAD) file. 

To implement an access method, an implementor will typically need to implement an AM-specific type of tuple table slot (see [ `src/include/executor/tuptable.h`](https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/include/executor/tuptable.h;hb=HEAD)), which allows code outside the access method to hold references to tuples of the AM, and to access the columns of the tuple. 

Currently, the way an AM actually stores data is fairly unconstrained. For example, it's possible, but not required, to use postgres' shared buffer cache. In case it is used, it likely makes sense to use PostgreSQL's standard page layout as described in [Section 65.6](storage-page-layout.md "65.6. Database Page Layout"). 

One fairly large constraint of the table access method API is that, currently, if the AM wants to support modifications and/or indexes, it is necessary for each tuple to have a tuple identifier (TID) consisting of a block number and an item number (see also [Section 65.6](storage-page-layout.md "65.6. Database Page Layout")). It is not strictly necessary that the sub-parts of TIDs have the same meaning they e.g., have for `heap`, but if bitmap scan support is desired (it is optional), the block number needs to provide locality. 

For crash safety, an AM can use postgres' [WAL](wal.md "Chapter 28. Reliability and the Write-Ahead Log"), or a custom implementation. If WAL is chosen, either [Generic WAL Records](generic-wal.md "63.1. Generic WAL Records") can be used, or a [Custom WAL Resource Manager](custom-rmgr.md "63.2. Custom WAL Resource Managers") can be implemented. 

To implement transactional support in a manner that allows different table access methods be accessed within a single transaction, it likely is necessary to closely integrate with the machinery in `src/backend/access/transam/xlog.c`. 

Any developer of a new `table access method` can refer to the existing `heap` implementation present in `src/backend/access/heap/heapam_handler.c` for details of its implementation. 

* * *

[Prev](geqo-biblio.md "60.4. Further Reading") | [Up](internals.md "Part VII. Internals")|  [Next](indexam.md "Chapter 62. Index Access Method Interface Definition")  
---|---|---  
60.4. Further Reading | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 62. Index Access Method Interface Definition
