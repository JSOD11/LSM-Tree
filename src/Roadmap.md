### Projects

## Main Projects

- Add DICT.

- Add RLE.

- Add DELTA.

- Add testing infrastructure.

- Add benchmarks and conduct experiments.

- Add an option to use leveling or tiering.


## Smaller projects

- Rearrange bloom filter bit distribution like in Monkey paper.

- Add an option to use leveling in the final level and tiering in all the others (like in Dostoevsky paper).


## Complete

- Out of place updates.

- Deletes.

- Restructure the design of levels to support keys and values that are different sizes.

- Refactor static arrays in the Catalog to a std::vector. Make LSM tree resize infinitely.

- Range queries.

- Bloom filters.

- Build the core of an LSM tree with correct outputs.
