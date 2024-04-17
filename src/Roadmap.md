### Projects

## Main Projects

- Range queries.

- Restructure the design of levels to support keys and values that are different sizes.

- Deletes.

- Delayed updates and deletes. Add a deletes bit, and when merging only include most recent entry.

- Add DICT.

- Add RLE.

- Add DELTA.

- Add an option to use leveling or tiering.


## Smaller projects

- Refactor static arrays in the Catalog to be std::vectors.

- Rearrange bloom filter bit distribution like in Monkey paper.

- Add an option to use leveling in the final level and tiering in all the others (like in Dostoevsky paper).


## Complete

- Bloom filters.

- Build the core of an LSM tree with correct outputs.
