import collections.abc
import csv
import logging
from abc import ABC
from pathlib import Path
from typing import Sequence

logger = logging.getLogger(__name__)


class AppendableSequence(collections.abc.Sequence, ABC):
    """Abstract base class for appendable sequences"""
    def append(self, _value):
        raise NotImplementedError


class NullLog(AppendableSequence):
    """Stub replacement for `ItemLog` that simply discards logged items
    and returns `False` for any containment checks."""
    def __len__(self) -> int:
        return 0

    def __getitem__(self, item):
        raise IndexError

    def __contains__(self, item):
        return False

    def append(self, _value):
        """This class just discards the given value"""
        pass


class ItemLog(AppendableSequence):
    """Log backed by a CSV file that is used to record item information,
    keyed by a particular column, with the ability to check whether a
    given key exists in the log already.

    `ItemLog` objects are iterable, and support direct indexing to a row
    by key.
    """
    def __init__(self, filename: str | Path, fieldnames: Sequence[str], keyfield: str, header: bool = True):
        self.filename: Path = Path(filename)
        self.fieldnames: Sequence[str] = fieldnames
        self.keyfield: str = keyfield
        self.write_header: bool = header
        self._item_keys = set()
        self._fh = None
        self._writer = None
        if self.exists():
            self._load_keys()

    @property
    def item_keys(self) -> set:
        return self._item_keys

    def exists(self) -> bool:
        """Returns `True` if the CSV log file exists."""
        return self.filename.is_file()

    def create(self):
        """Create the CSV log file. This will overwrite an existing file. If
        `write_header` is `True`, it will also write a header row to the file."""
        with self.filename.open(mode='w', buffering=1) as fh:
            writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
            if self.write_header:
                writer.writeheader()

    def _load_keys(self):
        for n, row in enumerate(iter(self), 1):
            try:
                self._item_keys.add(row[self.keyfield])
            except KeyError as e:
                raise ItemLogError(f'Key {e} not found in row {n}')

    def __iter__(self):
        try:
            with self.filename.open(mode='r', buffering=1) as fh:
                reader = csv.DictReader(fh)
                # check the validity of the map file data
                if not reader.fieldnames == self.fieldnames:
                    logger.warning(
                        f'Fieldnames in {self.filename} do not match expected fieldnames; '
                        f'expected: {self.fieldnames}; found: {reader.fieldnames}'
                    )
                # read the data from the existing file
                yield from reader
        except FileNotFoundError:
            # log file not found, so stop the iteration
            return

    @property
    def writer(self) -> csv.DictWriter:
        """CSV dictionary writer"""
        if not self.exists():
            self.create()
        if self._fh is None:
            self._fh = self.filename.open(mode='a', buffering=1)
        if self._writer is None:
            self._writer = csv.DictWriter(self._fh, fieldnames=self.fieldnames)
        return self._writer

    def append(self, row):
        """Write this `row` to the log."""
        self.writer.writerow(row)
        self._item_keys.add(row[self.keyfield])

    def writerow(self, row):
        """Alias for `append`"""
        self.append(row)

    def __contains__(self, other):
        return other in self._item_keys

    def __len__(self):
        return len(self._item_keys)

    def __getitem__(self, item):
        for n, row in enumerate(self):
            if n == item:
                return row
        raise IndexError(item)


class ItemLogError(Exception):
    pass
