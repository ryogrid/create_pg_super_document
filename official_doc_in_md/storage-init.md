65.5. The Initialization Fork  
---  
[Prev](storage-vm.md "65.4. Visibility Map") | [Up](storage.md "Chapter 65. Database Physical Storage")| Chapter 65. Database Physical Storage| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](storage-page-layout.md "65.6. Database Page Layout")  
  
* * *

## 65.5. The Initialization Fork #

Each unlogged table, and each index on an unlogged table, has an initialization fork. The initialization fork is an empty table or index of the appropriate type. When an unlogged table must be reset to empty due to a crash, the initialization fork is copied over the main fork, and any other forks are erased (they will be recreated automatically as needed). 

* * *

[Prev](storage-vm.md "65.4. Visibility Map") | [Up](storage.md "Chapter 65. Database Physical Storage")|  [Next](storage-page-layout.md "65.6. Database Page Layout")  
---|---|---  
65.4. Visibility Map | [Home](index.md "PostgreSQL 17.5 Documentation")|  65.6. Database Page Layout
