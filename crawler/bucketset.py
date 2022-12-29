"""A module for the 'bucket set'."""

import collections.abc
from typing import Callable, Generic, Iterable, Iterator, TypeVar

T = TypeVar("T")
K = TypeVar("K")


class BucketSet(collections.abc.MutableSet[T], Generic[T, K]):
    """A set that is split into buckets for performant operations."""

    def __init__(self, key: Callable[[T], K]):
        """Init the bucket set with a callable to retrieve the key."""
        self._key = key
        self._dict: dict[K, set[T]] = {}

    def __contains__(self, item: T) -> bool:
        """Check if item is contained in set."""
        key = self._key(item)
        return key in self._dict and item in self._dict[key]

    def __iter__(self) -> Iterator[T]:
        """Return iterator for all items in the set."""
        return (item for items in self._dict.values() for item in items)

    def __len__(self) -> int:
        """Compute the number of items in the set."""
        return sum(len(items) for items in self._dict.values())

    def add(self, item: T):
        """Add an item to the set."""
        key = self._key(item)
        if key not in self._dict:
            self._dict[key] = set()
        self._dict[key].add(item)

    def discard(self, item: T):
        """Remove an item from the set."""
        key = self._key(item)
        if key in self._dict:
            self._dict[key].discard(item)

    def update(self, iterable: Iterable[T]):
        """Add multiple items to the set."""
        for item in iterable:
            self.add(item)

    def key_difference(self, keys: set[K]) -> set[T]:
        """Return a normal set of all items stored in a different bucket."""
        return {
            item
            for key, items in self._dict.items()
            if key not in keys
            for item in items
        }
