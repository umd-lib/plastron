import logging
from argparse import FileType, Namespace
from datetime import datetime

from lxml import etree

from plastron.cli import get_uris
from plastron.repo.utils import context
from plastron.cli.commands import BaseCommand
from plastron.models.annotations import TextblockOnPage
from plastron.namespaces import pcdmuse
from plastron.ocr.alto import ALTOResource
from plastron.repo.pcdm import PCDMPageResource
from plastron.jobs import ItemLog

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='extractocr',
        description='Create annotations from OCR data stored in the repository'
    )
    parser.add_argument(
        '--ignore', '-i',
        help='file listing items to ignore',
        action='store'
    )
    parser.add_argument(
        '--no-transactions', '--no-txn',
        help='run the annotation process without using transactions',
        action='store_false',
        dest='use_transactions'
    )
    parser.add_argument(
        '--completed',
        help='file recording the URIs of processed resources',
        action='store'
    )
    parser.add_argument(
        '-f', '--file',
        dest='uris_file',
        type=FileType(mode='r'),
        help='File containing a list of URIs',
        action='store'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='Repository URIs'
    )
    parser.set_defaults(cmd_name='extractocr')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        # TODO: create an ExtractOCRJob

        fieldnames = ['uri', 'timestamp']

        # read the log of completed items
        if args.completed:
            completed = ItemLog(args.completed, fieldnames, 'uri')
            logger.info(f'Found {len(completed)} completed items')
        else:
            completed = []

        if args.ignore is not None:
            ignored = ItemLog(args.ignore, fieldnames, 'uri')
        else:
            ignored = []

        skipfile = f'logs/skipped.extractocr.{now}.csv'
        skipped = ItemLog(skipfile, fieldnames, 'uri')

        for uri in get_uris(args):
            if uri in completed:
                logger.info(f'Resource {uri} has been completed; skipping')
                continue
            elif uri in ignored:
                logger.info(f'Ignoring {uri}')
                continue

            with context(repo=self.context.repo, use_transactions=args.use_transactions):
                page_resource = self.context.repo[uri:PCDMPageResource].read()
                extracted_text_file = page_resource.get_file(rdf_type=pcdmuse.ExtractedText)
                if extracted_text_file is None:
                    logger.error(f'Resource {page_resource.url} has no OCR file; skipping')
                    skipped.append({'uri': uri, 'timestamp': str(datetime.utcnow())})
                    continue

                # TODO: currently assuming all extracted text files are ALTO
                # TODO: must add format determining code to support hOCR
                with extracted_text_file.open() as fh:
                    xmldoc = etree.parse(fh)

                alto = ALTOResource(xmldoc, (400, 400))
                for textblock in alto.textblocks:
                    # TODO: better argument structuring
                    annotation = TextblockOnPage.from_textblock(
                        textblock=textblock,
                        page=page_resource,
                        scale=alto.scale,
                        ocr_file=extracted_text_file,
                    )
                    annotation_resource = page_resource.create_annotation(description=annotation)
                    print(annotation_resource.url)
