"""handler for loading reel objects created by the ndnp handler"""

import csv
import logging
import os

from plastron.models.newspaper import Reel, Page
from plastron.rdf import pcdm


class Batch:
    def __init__(self, repo, config):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )
        self.repo = repo
        self.collection = pcdm.Collection.from_repository(repo, config.collection_uri)

        with os.scandir(config.batch_file) as files:
            self.files = [entry.path for entry in files if
                          entry.name.endswith('.csv')]

        self.length = len(self.files)
        self.num = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.num < self.length:
            item = BatchItem(self, self.files[self.num])
            self.num += 1
            return item
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()


class BatchItem:
    def __init__(self, batch, filename):
        self.batch = batch
        self.path = filename

    def read_data(self):
        id = os.path.splitext(os.path.basename(self.path))[0]
        reel = Reel(id=id, title=f'Reel Number {id}', member_of=self.batch.collection)

        with open(self.path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                reel.add_member(Page.from_repository(self.batch.endpoint, row['uri']))

        return reel
