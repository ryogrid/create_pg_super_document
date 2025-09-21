E.4. Release 17.2  
---  
[Prev](release-17-3.md "E.3. Release 17.3") | [Up](release.md "Appendix E. Release Notes")| Appendix E. Release Notes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](release-17-1.md "E.5. Release 17.1")  
  
* * *

## E.4. Release 17.2 #

[E.4.1. Migration to Version 17.2](release-17-2.md#RELEASE-17-2-MIGRATION)
[E.4.2. Changes](release-17-2.md#RELEASE-17-2-CHANGES)

**Release date:** 2024-11-21

This release contains a few fixes from 17.1. For information about new features in major release 17, see [Section E.6](release-17.md "E.6. Release 17"). 

### E.4.1. Migration to Version 17.2 #

A dump/restore is not required for those running 17.X. 

However, if you are upgrading from a version earlier than 17.1, see [Section E.5](release-17-1.md "E.5. Release 17.1"). 

### E.4.2. Changes #

  * Repair ABI break for extensions that work with struct `ResultRelInfo` (Tom Lane) [§](https://postgr.es/c/6bfacd368)

Last week's minor releases unintentionally broke binary compatibility with timescaledb and several other extensions. Restore the affected structure to its previous size, so that such extensions need not be rebuilt. 

  * Restore functionality of `ALTER {ROLE|DATABASE} SET role` (Tom Lane, Noah Misch) [§](https://postgr.es/c/1c05004a8)

The fix for CVE-2024-10978 accidentally caused settings for `role` to not be applied if they come from non-interactive sources, including previous `ALTER {ROLE|DATABASE}` commands and the `PGOPTIONS` environment variable. 

  * Fix cases where a logical replication slot's `restart_lsn` could go backwards (Masahiko Sawada) [§](https://postgr.es/c/568e78a65)

Previously, restarting logical replication could sometimes cause the slot's restart point to be recomputed as an older value than had previously been advertised in `pg_replication_slots`. This is bad, since for example WAL files might have been removed on the basis of the later `restart_lsn` value, in which case replication would fail to restart. 

  * Avoid deleting still-needed WAL files during pg_rewind (Polina Bungina, Alexander Kukushkin) [§](https://postgr.es/c/cb844d66b)

Previously, in unlucky cases, it was possible for pg_rewind to remove important WAL files from the rewound demoted primary. In particular this happens if those files have been marked for archival (i.e., their `.ready` files were created) but not yet archived. Then the newly promoted node no longer has such files because of them having been recycled, but likely they are needed for recovery in the demoted node. If pg_rewind removes them, recovery is not possible anymore. 

  * Fix race conditions associated with dropping shared statistics entries (Kyotaro Horiguchi, Michael Paquier) [§](https://postgr.es/c/1d6a03ea4)

These bugs could lead to loss of statistics data, assertion failures, or “can only drop stats once” errors. 

  * Count index scans in `contrib/bloom` indexes in the statistics views, such as the `pg_stat_user_indexes`.`idx_scan` counter (Masahiro Ikeda) [§](https://postgr.es/c/7af6d1306)

  * Fix crash when checking to see if an index's opclass options have changed (Alexander Korotkov) [§](https://postgr.es/c/a6fa869cf)

Some forms of `ALTER TABLE` would fail if the table has an index with non-default operator class options. 

  * Avoid assertion failure caused by disconnected NFA sub-graphs in regular expression parsing (Tom Lane) [§](https://postgr.es/c/5f28e6ba7)

This bug does not appear to have any visible consequences in non-assert builds. 




* * *

[Prev](release-17-3.md "E.3. Release 17.3") | [Up](release.md "Appendix E. Release Notes")|  [Next](release-17-1.md "E.5. Release 17.1")  
---|---|---  
E.3. Release 17.3 | [Home](index.md "PostgreSQL 17.5 Documentation")|  E.5. Release 17.1
