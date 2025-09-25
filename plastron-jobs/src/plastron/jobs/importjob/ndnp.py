"""Classes for reading metadata and files stored according to the NDNP specification."""

import logging
import sys
from csv import DictWriter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Any

from lxml import etree
# noinspection PyProtectedMember
from lxml.etree import XMLSyntaxError, _ElementTree, QName, _Element

from plastron.files import FileSpec
from plastron.repo import DataReadError

logger = logging.getLogger(__name__)

ISSUE_FIELDNAMES = ['Title', 'Date', 'Volume', 'Issue', 'Edition', 'Rights Statement', 'FILES', 'ITEM_FILES']


class XMLNS:
    """Convenience class for constructing namespaced XML element name strings
    for use with the `lxml.ElementTree.find()` method.

    ```pycon
    >>> mets = XMLNS('http://www.loc.gov/METS/')

    >>> str(mets)
    'http://www.loc.gov/METS/'

    >>> mets.Flocat
    '{http://www.loc.gov/METS/}Flocat'

    ```
    """
    def __init__(self, uri: str):
        self.uri = uri
        """Namespace URI"""

    def __str__(self):
        return self.uri

    def __getattr__(self, item):
        return str(QName(self.uri, str(item)))


METS = XMLNS('http://www.loc.gov/METS/')
"""`XMLNS` object for the METS namespace"""

xlink = XMLNS('http://www.w3.org/1999/xlink')
"""`XMLNS` object for the xlink namespace"""

xmlns = {
    'METS': str(METS),
    'mix': 'http://www.loc.gov/mix/',
    'MODS': 'http://www.loc.gov/mods/v3',
    'premis': 'http://www.loc.gov/standards/premis',
    'xlink': str(xlink),
}
"""Mapping of XML prefix to namespace URIs"""


@dataclass
class NDNPIssue:
    """Class representing a single newspaper issue in
    [NDNP](https://www.loc.gov/ndnp/) format."""

    batch: 'NDNPBatch'
    lccn: str
    issue_date: str
    edition_order: int
    mets_path: Path
    article_mets_path: Path
    _mets_doc: _ElementTree = None
    _mets: 'METSResource' = None

    @property
    def mets_doc(self) -> _ElementTree:
        """The `lxml.ElementTree` representation of this issue's METS file."""
        if self._mets_doc is None:
            self._mets_doc = etree.parse(self.mets_path)
        return self._mets_doc

    @property
    def mets(self) -> 'METSResource':
        """The `METSResource` representation of this issue's METS file."""
        if self._mets is None:
            self._mets = METSResource(self.mets_doc)
        return self._mets

    def get_title(self) -> str:
        """Get the issue title as a string."""
        return self.mets_doc.getroot().get('LABEL')

    def _get_detail_number(self, type_attr: str) -> Optional[str]:
        try:
            return self.mets_doc.find(f'.//MODS:detail[@type="{type_attr}"]/MODS:number', namespaces=xmlns).text
        except AttributeError:
            return None

    def get_volume(self) -> Optional[str]:
        """Get the issue's volume number as a string, or `None` if it cannot be found."""
        return self._get_detail_number('volume')

    def get_issue(self) -> Optional[str]:
        """Get the issue's issue number as a string, or `None` if it cannot be found."""
        return self._get_detail_number('issue')

    def get_edition(self) -> Optional[str]:
        """Get the issue's edition number as a string, or `None` if it cannot be found."""
        return self._get_detail_number('edition')


class NDNPBatch:
    """Class representing a batch of newspaper issues in NDNP format."""
    root_dir: Path
    """Root directory of the NDNP package"""
    batch_file: Path
    """Main XML file describing this NDNP package. Defaults to `batch.xml`
    in the batch's `root_dir`"""

    def __init__(self, batch_dir: str | Path, batch_file: str = 'batch.xml'):
        self.root_dir = Path(batch_dir)
        if not self.root_dir.is_dir():
            raise DataReadError(f'{self.root_dir} is not a directory')
        self.batch_file = self.root_dir / batch_file
        if not self.batch_file.is_file():
            raise DataReadError(f'{self.batch_file} does not exist, or is not a file')
        try:
            self.xmldoc = etree.parse(self.batch_file)
        except OSError:
            raise DataReadError(f'Unable to read {self.batch_file}')
        except XMLSyntaxError:
            raise DataReadError(f'Unable to parse {self.batch_file} as XML')

    def issues(self) -> Iterator[NDNPIssue]:
        """Iterator of `NDNPIssue` objects constructed by parsing the `NDNPBatch`
        METS metadata files."""

        for issue in self.xmldoc.xpath('//ndnp:issue', namespaces={'ndnp': 'http://www.loc.gov/ndnp'}):
            issue_filename = issue.text
            # strips out the trailing "_1" from the file basename
            # seems to be specific to our datasets?
            article_filename = issue_filename[:-6] + issue_filename[-4:]
            yield NDNPIssue(
                batch=self,
                lccn=issue.get('lccn'),
                issue_date=issue.get('issueDate'),
                edition_order=int(issue.get('editionOrder')),
                mets_path=self.root_dir / issue_filename,
                article_mets_path=self.root_dir / 'Article-Level' / article_filename,
            )


def write_import_csv(batch: NDNPBatch, fh=None):
    """Iterates over issues in the given `NDNPBatch` and uses `get_issue_data()`
    to get mapping data for writing to the given file handle `fh` in CSV format.
    If no `fh` is given, it uses `sys.stdout` to write the CSV file to STDOUT."""

    if fh is None:
        fh = sys.stdout
    writer = DictWriter(fh, fieldnames=ISSUE_FIELDNAMES)
    writer.writeheader()
    for issue in batch.issues():
        row = get_issue_data(issue)
        writer.writerow(row)


def get_issue_data(issue: NDNPIssue) -> dict[str, str]:
    """Transforms an `NDNPIssue` into a mapping suitable for writing to a
    CSV file using a `csv.DictWriter`"""

    # get item-level files: METS metadata for the issue and for the articles
    item_files = [
        FileSpec(
            name=str(issue.mets_path.relative_to(issue.batch.root_dir)),
            usage='metadata'
        ),
        FileSpec(
            name=str(issue.article_mets_path.relative_to(issue.batch.root_dir)),
            usage='metadata'
        ),
    ]
    # get pages and page-level files
    files = []
    for page in issue.mets.pages():
        for file_id in page.get_file_ids():
            file = issue.mets.file(file_id)
            file_href = file.find(METS.FLocat).get(xlink.href)
            if file.get('USE') == 'master':
                file_use = 'preservation'
            else:
                file_use = file.get('USE')
            file_path = issue.mets_path.parent / file_href
            files.append(FileSpec(name=str(file_path.relative_to(issue.batch.root_dir)), usage=file_use))

    return {
        'Title': issue.get_title(),
        'Date': issue.issue_date,
        'Volume': issue.get_volume(),
        'Issue': issue.get_issue(),
        'Edition': issue.get_edition(),
        'Rights Statement': 'http://vocab.lib.umd.edu/rightsStatement#InC-NC',
        'FILES': ';'.join(f.spec for f in files),
        'ITEM_FILES': ';'.join(f.spec for f in item_files),
    }


def get_article_data(article_path) -> Iterator[dict[str, Any]]:
    """**Note:** Not currently used, may be deprecated in the future.

    Iterates over the article METS XML file and extract the metadata
    for each article."""
    try:
        article_tree = etree.parse(article_path)
    except OSError:
        raise DataReadError(f"Unable to read {article_path}")
    except XMLSyntaxError:
        raise DataReadError(f"Unable to parse {article_path} as XML")

    article_root = article_tree.getroot()
    for article in article_root.findall(METS.div + '[@TYPE="article"]'):
        article_title = article.get('LABEL')
        page_numbers = sorted(list(set(
            int(area.get('FILEID').replace('ocrFile', ''))
            for area in article.findall(METS.area)
        )))
        yield {
            'Title': article_title,
            'First page': page_numbers[0],
            'Last page': page_numbers[-1],
        }


class METSDiv:
    """Class wrapping a `METS:div` element."""

    def __init__(self, element: _Element):
        self.element = element

    def get_file_ids(self) -> Iterator[str]:
        """Iterator of the file ID (`@FILEID`) attributes for all
        file pointer elements (`METS:fptr`) in this `METS:div`."""
        for fptr in self.element.findall(METS.fptr):
            yield fptr.get('FILEID')


class METSResource:
    def __init__(self, xmldoc: _ElementTree):
        self.root = xmldoc.getroot()
        self.xpath = etree.XPathElementEvaluator(
            self.root,
            namespaces=xmlns,
            smart_strings=False,
        )

    def pages(self) -> Iterator[METSDiv]:
        """Iterator of `METSDiv` objects representing all `METS:div` elements
        with the type `np:page` in this resource."""
        for page_div in self.xpath('METS:structMap//METS:div[@TYPE="np:page"]'):
            yield METSDiv(page_div)

    def dmdsec(self, id: str) -> _Element:
        """Get the descriptive metadata section element (`METS:dmdSec`) with the
        given `id`. Raises a `DataReadError` if such an element cannot be found."""
        try:
            return self.xpath('METS:dmdSec[@ID=$id]', id=id)[0]
        except IndexError:
            raise DataReadError(f'Cannot find METS:dmdSec element with ID "{id}"')

    def file(self, id: str) -> _Element:
        """Get the file element (`METS:file`) with the given `id`. Raises a
        `DataReadError` if such an element cannot be found."""
        try:
            return self.xpath('METS:fileSec//METS:file[@ID=$id]', id=id)[0]
        except IndexError:
            raise DataReadError(f'Cannot find METS:file element with ID "{id}"')

    def techmd(self, id: str) -> _Element:
        """Get the technical metadata element (`METS:techMD`) with the given `id`.
        Raises a `DataReadError` if such an element cannot be found."""
        try:
            return self.xpath('METS:amdSec/METS:techMD[@ID=$id]', id=id)[0]
        except IndexError:
            raise DataReadError(f'Cannot find METS:techMD element with ID "{id}"')
