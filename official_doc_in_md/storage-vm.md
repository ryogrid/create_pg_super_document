65.4. Visibility Map  
---  
[Prev](storage-fsm.md "65.3. Free Space Map") | [Up](storage.md "Chapter 65. Database Physical Storage")| Chapter 65. Database Physical Storage| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](storage-init.md "65.5. The Initialization Fork")  
  
* * *

## 65.4. Visibility Map #

Each heap relation has a Visibility Map (VM) to keep track of which pages contain only tuples that are known to be visible to all active transactions; it also keeps track of which pages contain only frozen tuples. It's stored alongside the main relation data in a separate relation fork, named after the filenode number of the relation, plus a `_vm` suffix. For example, if the filenode of a relation is 12345, the VM is stored in a file called `12345_vm`, in the same directory as the main relation file. Note that indexes do not have VMs. 

The visibility map stores two bits per heap page. The first bit, if set, indicates that the page is all-visible, or in other words that the page does not contain any tuples that need to be vacuumed. This information can also be used by [_index-only scans_](indexes-index-only-scans.md "11.9. Index-Only Scans and Covering Indexes") to answer queries using only the index tuple. The second bit, if set, means that all tuples on the page have been frozen. That means that even an anti-wraparound vacuum need not revisit the page. 

The map is conservative in the sense that we make sure that whenever a bit is set, we know the condition is true, but if a bit is not set, it might or might not be true. Visibility map bits are only set by vacuum, but are cleared by any data-modifying operations on a page. 

The [pg_visibility](pgvisibility.md "F.34. pg_visibility — visibility map information and utilities") module can be used to examine the information stored in the visibility map. 

* * *

[Prev](storage-fsm.md "65.3. Free Space Map") | [Up](storage.md "Chapter 65. Database Physical Storage")|  [Next](storage-init.md "65.5. The Initialization Fork")  
---|---|---  
65.3. Free Space Map | [Home](index.md "PostgreSQL 17.5 Documentation")|  65.5. The Initialization Fork
