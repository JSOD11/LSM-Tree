### Projects

## Main Projects

- Bloom filters.

- Range queries.

- Deletes.

- Delayed updates and deletes. Add a deletes bit, and when merging only include most recent entry.

- Add an option to use leveling or tiering.


## Smaller projects

- Refactor static arrays in the Catalog to be std::vectors.

- Rearrange bloom filter bit distribution like in Monkey paper.

- Add an option to use leveling in the final level and tiering in all the others (like in Dostoevsky paper).


## Complete

- Build the core of an LSM tree with correct outputs.
