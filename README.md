## When to use this?

- You want to store blobs. Blobs are huge files, like 10mb+.
- You will have read-mostly access pattern.
- You want get_range(from, to) functionality.
- You want 0 write amplification. You will have exactly one 1 write per blob
  without compactions and all of this stuff.
- You tried to use rocksdb for this, and it killed your disk and brain :)

## When not to use this?

- Lots of small files. Lsm tree is still a king here.
- You use something not unix-like.

## Architecture principles

- Firstly store data in cas, then index it. With this approach you don't need
  to handle consistency errors in the user code. You can get list of orphaned
  blobs on startup and do something with them.
  As user, you can do nothing if you have a record in the index for blob, but no
  actual blob in cas.
  Hi mr. rocksdb :)

- Blobs are stored in a classic cas manner /h/a/s/h.
- Hash is blake3
- Index is stored in a single file. Which is periodically rewritten on wal
  roll-over.
- Internally index is a BtreeMap.

## Concurrency and Locking

Cassadilia uses a two-level locking strategy to ensure consistency:

### 1. Filesystem Operations Lock (`FsLock`)

A single shared mutex protects CAS fs operations:

- **Directory creation**: Prevents races when multiple threads create the same
  hash prefix directory
- **Blob commits**: Ensures atomic renames when moving staged files to
  CAS
- **Blob deletions**: Ensures multiple blob deletions happen atomically as a
  group

The lock is held only during filesystem metadata operations, not during index
updates or blob content writes.

### 2. Intent Tracking System

To prevent race conditions between concurrent commits and deletions, Cassadilia
uses an intent tracking system / 2-phase commit protocol:

1. **Before committing a blob to CAS**, a transaction registers its intent with
   the index:
    - Increments the blob's reference count immediately
    - Tracks the pending intent in the index

2. **When the blob is moved in CAS**, the transaction:
    - Writes to the WAL
    - Updates the index state
    - Removes the intent

3. **During deletion**, the system checks both:
    - Committed reference counts in the index
    - Pending intents that haven't been committed yet

This prevents the race where Thread A could delete a blob that Thread B is in
the process of committing.

## Todo

- [ ] Gather orphaned blobs on startup.
- [ ] Allow to check blobs integrity.
- [ ] Gc empty directories
- [ ] Cache fd-s
- [ ] Use mmap or allow to configure read mode
- [ ] Lock index to disallow concurrent db open
- [x] Save settings in the separate file