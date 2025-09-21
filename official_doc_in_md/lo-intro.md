33.1. Introduction  
---  
[Prev](largeobjects.md "Chapter 33. Large Objects") | [Up](largeobjects.md "Chapter 33. Large Objects")| Chapter 33. Large Objects| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](lo-implementation.md "33.2. Implementation Features")  
  
* * *

## 33.1. Introduction #

All large objects are stored in a single system table named [`pg_largeobject`](catalog-pg-largeobject.md "51.30. pg_largeobject"). Each large object also has an entry in the system table [`pg_largeobject_metadata`](catalog-pg-largeobject-metadata.md "51.31. pg_largeobject_metadata"). Large objects can be created, modified, and deleted using a read/write API that is similar to standard operations on files. 

PostgreSQL also supports a storage system called [“TOAST”](storage-toast.md "65.2. TOAST"), which automatically stores values larger than a single database page into a secondary storage area per table. This makes the large object facility partially obsolete. One remaining advantage of the large object facility is that it allows values up to 4 TB in size, whereas TOASTed fields can be at most 1 GB. Also, reading and updating portions of a large object can be done efficiently, while most operations on a TOASTed field will read or write the whole value as a unit. 

* * *

[Prev](largeobjects.md "Chapter 33. Large Objects") | [Up](largeobjects.md "Chapter 33. Large Objects")|  [Next](lo-implementation.md "33.2. Implementation Features")  
---|---|---  
Chapter 33. Large Objects | [Home](index.md "PostgreSQL 17.5 Documentation")|  33.2. Implementation Features
