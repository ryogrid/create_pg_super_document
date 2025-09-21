Chapter 65. Database Physical Storage  
---  
[Prev](hash-index.md "64.6. Hash Indexes") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](storage-file-layout.md "65.1. Database File Layout")  
  
* * *

## Chapter 65. Database Physical Storage

**Table of Contents**

[65.1. Database File Layout](storage-file-layout.md)
[65.2. TOAST](storage-toast.md)
    

[65.2.1. Out-of-Line, On-Disk TOAST Storage](storage-toast.md#STORAGE-TOAST-ONDISK)
[65.2.2. Out-of-Line, In-Memory TOAST Storage](storage-toast.md#STORAGE-TOAST-INMEMORY)
[65.3. Free Space Map](storage-fsm.md)
[65.4. Visibility Map](storage-vm.md)
[65.5. The Initialization Fork](storage-init.md)
[65.6. Database Page Layout](storage-page-layout.md)
    

[65.6.1. Table Row Layout](storage-page-layout.md#STORAGE-TUPLE-LAYOUT)
[65.7. Heap-Only Tuples (HOT)](storage-hot.md)

This chapter provides an overview of the physical storage format used by PostgreSQL databases. 

* * *

[Prev](hash-index.md "64.6. Hash Indexes") | [Up](internals.md "Part VII. Internals")|  [Next](storage-file-layout.md "65.1. Database File Layout")  
---|---|---  
64.6. Hash Indexes | [Home](index.md "PostgreSQL 17.5 Documentation")|  65.1. Database File Layout
