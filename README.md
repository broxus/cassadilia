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

## Todo

- [ ] Gather orphaned blobs on startup.
- [ ] Allow to check blobs integrity.
- [ ] Gc empty directories
- [ ] Cache fd-s
- [ ] Use mmap or allow to configure read mode
- [ ] Lock index to disallow concurrent db open
- [ ] Save settings in the separate file