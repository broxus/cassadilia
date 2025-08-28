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

We use scoped intents to prevent races between concurrent puts and
deletes.

- Register intent: record `key -> blob_hash` in `pending_intents` map; no
  refcount changes.
- Commit: append WAL Put, apply it to the index updating refcounts, remove the
  intent, compute unreferenced blobs, exclude hashes still referenced by active
  intents, delete the rest.
- Drop without commit: remove the current intent and restore any previously
  replaced intent; no refcount changes.
- Remove ops: append WAL Remove, apply to state, filter unreferenced blobs
  against active intents, delete before doing checkpoint.

Intents are in-memory only; if a process exits before commit, any staged files
become orphans, handled by startup orphan scanning.

## Todo

- [x] Gather orphaned blobs on startup.
- [x] Allow to check blobs integrity.
- [ ] Gc empty directories
- [ ] Cache fd-s
- [ ] Use mmap or allow to configure read mode
- [x] Lock index file to disallow concurrent db open
- [x] Save settings in the separate file
