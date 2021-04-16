""" Classes for interpreting and loading metadata and files stored
    according to the NDNP specification. """

import csv
import logging
import lxml
from lxml.etree import parse, XMLSyntaxError
import os
from plastron import pcdm
from plastron.exceptions import DataReadException
from plastron.namespaces import dcmitype, ndnp
from plastron.files import LocalFileSource
from plastron.models.newspaper import Article, Issue, IssueMetadata, MetadataFile, Page

# alias the rdflib Namespace
ns = ndnp

# ============================================================================
# METADATA MAPPING
# ============================================================================

XPATHMAP = {
    'batch': {
        'issues': "./{http://www.loc.gov/ndnp}issue",
        'reels': "./{http://www.loc.gov/ndnp}reel"
    },

    'issue': {
        'volume': (".//{http://www.loc.gov/mods/v3}detail[@type='volume']/"
                   "{http://www.loc.gov/mods/v3}number"
                   ),
        'issue': (".//{http://www.loc.gov/mods/v3}detail[@type='issue']/"
                  "{http://www.loc.gov/mods/v3}number"
                  ),
        'edition': (".//{http://www.loc.gov/mods/v3}detail[@type='edition']/"
                    "{http://www.loc.gov/mods/v3}number"
                    ),
        'article': (".//{http://www.loc.gov/METS/}div[@TYPE='article']"
                    ),
        'areas': (".//{http://www.loc.gov/METS/}area"
                  ),
    }
}

xmlns = {
    'METS': 'http://www.loc.gov/METS/',
    'mix': 'http://www.loc.gov/mix/',
    'MODS': 'http://www.loc.gov/mods/v3',
    'premis': 'http://www.loc.gov/standards/premis',
    'xlink': 'http://www.w3.org/1999/xlink',
}


# ============================================================================
# NDNP BATCH CLASS
# ============================================================================

class Batch:
    def __init__(self, repo, config):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )
        graph = repo.get_graph(config.collection_uri, include_server_managed=False)
        self.collection = pcdm.Collection.from_graph(graph, config.collection_uri)
        self.collection.created = True

        self.fieldnames = ['aggregation', 'sequence', 'uri']

        try:
            tree = parse(config.batch_file)
        except OSError:
            raise DataReadException(f'Unable to read {config.batch_file}')
        except XMLSyntaxError:
            raise DataReadException(f'Unable to parse {config.batch_file} as XML')

        root = tree.getroot()
        m = XPATHMAP

        # read over the index XML file assembling a list of paths to the issues
        self.basepath = os.path.dirname(config.batch_file)
        self.issues = []
        for i in root.findall(m['batch']['issues']):
            sanitized_path = i.text[:-6] + i.text[-4:]
            self.issues.append(
                (os.path.join(self.basepath, i.text),
                 os.path.join(
                     self.basepath, "Article-Level", sanitized_path)
                 )
            )

        # set up a CSV file for each reel, skipping existing CSVs
        self.reels = set(
            [r.get('reelNumber') for r in root.findall(m['batch']['reels'])]
        )
        self.logger.info('Batch contains {0} reels'.format(len(self.reels)))
        self.path_to_reels = os.path.join(config.log_dir, 'reels')
        if not os.path.isdir(self.path_to_reels):
            os.makedirs(self.path_to_reels)
        for n, reel in enumerate(self.reels):
            reel_csv = '{0}/{1}.csv'.format(self.path_to_reels, reel)
            if not os.path.isfile(reel_csv):
                self.logger.info(
                    "{0}. Creating reel aggregation CSV in '{1}'".format(
                        n + 1, reel_csv)
                )
                with open(reel_csv, 'w') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    writer.writeheader()
            else:
                self.logger.info(
                    "{0}. Reel aggregation file '{1}' exists; skipping".format(
                        n + 1, reel_csv)
                )

        self.length = len(self.issues)
        self.num = 0
        self.logger.info("Batch contains {0} items.".format(self.length))

    def __iter__(self):
        return self

    def __next__(self):
        if self.num < self.length:
            issue_path, article_path = self.issues[self.num]
            item = BatchItem(self, issue_path, article_path)
            self.num += 1
            return item
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()


# mapping from the USE attribute to a class representing that type of file
FILE_CLASS_FOR = {
    'master': pcdm.PreservationMasterFile,
    'service': pcdm.IntermediateFile,
    'derivative': pcdm.ServiceFile,
    'ocr': pcdm.ExtractedText
}


class BatchItem:
    def __init__(self, batch, issue_path, article_path):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )

        self.batch = batch
        self.issue = None
        # gather metadata
        self.dir = os.path.dirname(issue_path)
        self.path = issue_path
        self.article_path = article_path
        self.reel_csv_loc = batch.path_to_reels

    def read_data(self):
        try:
            tree = parse(self.path)
        except OSError:
            raise DataReadException("Unable to read {0}".format(self.path))
        except XMLSyntaxError:
            raise DataReadException(
                "Unable to parse {0} as XML".format(self.path)
            )

        issue_mets = METSResource(tree)
        root = tree.getroot()
        m = XPATHMAP['issue']

        issue = Issue(member_of=self.batch.collection)

        # get required metadata elements
        try:
            issue.title = root.get('LABEL')
            issue.date = root.find('.//MODS:dateIssued', xmlns).text
            issue.sequence_attr = ('Page', 'number')
        except AttributeError:
            raise DataReadException("Missing metadata in {0}".format(self.path))

        # optional metadata elements
        if root.find(m['volume']) is not None:
            issue.volume = root.find(m['volume']).text
        if root.find(m['issue']) is not None:
            issue.issue = root.find(m['issue']).text
        if root.find(m['edition']) is not None:
            issue.edition = root.find(m['edition']).text

        # add the issue and article-level XML files as related objects
        issue.add_related(IssueMetadata(MetadataFile.from_source(
            LocalFileSource(self.path),
            title=f'{issue.title}, issue METS metadata'
        )))
        issue.add_related(IssueMetadata(MetadataFile.from_source(
            LocalFileSource(self.article_path),
            title=f'{issue.title}, article METS metadata'
        )))

        # create a page object for each page and append to list of pages
        for page_div in issue_mets.xpath('METS:structMap//METS:div[@TYPE="np:page"]'):
            # create a page and add to the list of members
            page = self.create_page(issue_mets, page_div, issue)
            issue.add_member(page)

            # create a proxy for the page in this issue and add it to the aggregation
            issue.append_proxy(page, title=f'Proxy for page {page.number} in {issue.title}')

            # add OCR text blocks as annotations
            issue.annotations.extend(page.textblocks())

        # iterate over the article XML and create objects for articles
        try:
            article_tree = parse(self.article_path)
        except OSError:
            raise DataReadException(
                "Unable to read {0}".format(self.article_path)
            )
        except XMLSyntaxError:
            raise DataReadException(
                "Unable to parse {0} as XML".format(self.article_path)
            )

        article_root = article_tree.getroot()
        for article in article_root.findall(m['article']):
            article_title = article.get('LABEL')
            article_pagenums = set()
            for area in article.findall(m['areas']):
                pagenum = int(area.get('FILEID').replace('ocrFile', ''))
                article_pagenums.add(pagenum)
            article = Article(
                title=article_title,
                issue=issue,
                pages=sorted(list(article_pagenums))
            )
            issue.add_member(article)

        self.issue = issue
        return issue

    def create_page(self, issue_mets, page_div, issue):
        dmdsec = issue_mets.dmdsec(page_div.get('DMDID'))
        number = dmdsec.find('.//MODS:start', xmlns).text
        reel = dmdsec.find('.//MODS:identifier[@type="reel number"]', xmlns)
        if reel is not None:
            reel = reel.text
        frame = dmdsec.find('.//MODS:identifier[@type="reel sequence number"]', xmlns)
        if frame is not None:
            frame = frame.text
        title = "{0}, page {1}".format(issue.title, number)

        # create Page object
        page = Page(issue=issue, reel=reel, number=number, title=title, frame=frame)

        # optionally generate a file object for each file in the XML snippet
        for fptr in page_div.findall('METS:fptr', xmlns):
            fileid = fptr.get('FILEID')
            filexml = issue_mets.file(fileid)

            if 'ADMID' not in filexml.attrib:
                raise DataReadException(f'No ADMID found for {fileid}, cannot lookup technical metadata')

            # get technical metadata by type
            techmd = {}
            for admid in filexml.get('ADMID').split():
                t = issue_mets.techmd(admid)
                for mdwrap in t.findall('METS:mdWrap', xmlns):
                    mdtype = mdwrap.get('MDTYPE')
                    if mdtype == 'OTHER':
                        mdtype = mdwrap.get('OTHERMDTYPE')
                    techmd[mdtype] = t

            use = filexml.get('USE')
            file_locator = filexml.find('METS:FLocat', xmlns)
            href = file_locator.get('{http://www.w3.org/1999/xlink}href')
            localpath = os.path.join(self.dir, os.path.basename(href))
            basename = os.path.basename(localpath)
            mimetype = techmd['PREMIS'].find('.//premis:formatName', xmlns).text

            file_class = FILE_CLASS_FOR[use]

            file = file_class.from_source(
                LocalFileSource(localpath, mimetype=mimetype),
                title=f'{basename} ({use})'
            )
            file.use = use
            file.basename = basename
            file.dcmitype = dcmitype.Text

            if mimetype == 'image/tiff':
                file.width = techmd['NISOIMG'].find('.//mix:ImageWidth', xmlns).text
                file.height = techmd['NISOIMG'].find('.//mix:ImageLength', xmlns).text
                file.resolution = (
                    int(techmd['NISOIMG'].find('.//mix:XSamplingFrequency', xmlns).text),
                    int(techmd['NISOIMG'].find('.//mix:YSamplingFrequency', xmlns).text)
                )
            else:
                file.width = None
                file.height = None
                file.resolution = None

            page.add_file(file)

        page.parse_ocr()

        return page

    # actions to take upon successful creation of object in repository
    def post_creation_hook(self):
        for page in self.issue.ordered_components():
            if hasattr(page, 'frame'):
                row = {'aggregation': page.reel,
                       'sequence': page.frame,
                       'uri': page.uri
                       }
                csv_path = os.path.join(
                    self.reel_csv_loc, '{0}.csv'.format(page.reel)
                )
                with open(csv_path, 'r') as f:
                    fieldnames = f.readline().strip('\n').split(',')
                with open(csv_path, 'a') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)
        self.logger.info('Completed post-creation actions')


class METSResource(object):
    def __init__(self, xmldoc):
        self.root = xmldoc.getroot()
        self.xpath = lxml.etree.XPathElementEvaluator(self.root, namespaces=xmlns,
                                                      smart_strings=False)

    def dmdsec(self, id):
        try:
            return self.xpath('METS:dmdSec[@ID=$id]', id=id)[0]
        except IndexError:
            raise DataReadException(f'Cannot find METS:dmdSec element with ID "{id}"')

    def file(self, id):
        try:
            return self.xpath('METS:fileSec//METS:file[@ID=$id]', id=id)[0]
        except IndexError:
            raise DataReadException(f'Cannot find METS:file element with ID "{id}"')

    def techmd(self, id):
        try:
            return self.xpath('METS:amdSec/METS:techMD[@ID=$id]', id=id)[0]
        except IndexError:
            raise DataReadException(f'Cannot find METS:techMD element with ID "{id}"')
