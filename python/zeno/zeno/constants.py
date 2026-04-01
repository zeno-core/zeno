"""Constants for zeno KV store."""

from __future__ import annotations

# Key constraints
MAX_KEY_LEN = 4096  # Maximum key length in bytes
NUM_SHARDS = 256  # Number of shards (2^8)

# ART node constraints
MAX_PREFIX_LEN = 11  # Maximum inline prefix bytes

# Node capacities
NODE4_CAPACITY = 4
NODE16_CAPACITY = 16
NODE48_CAPACITY = 48
NODE256_CAPACITY = 256

# Node type thresholds for growth/shrink
NODE4_MAX = 4
NODE16_MIN = 5
NODE16_MAX = 16
NODE48_MIN = 17
NODE48_MAX = 48
NODE256_MIN = 49
