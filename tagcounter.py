import os
import settings
import operator

from queue import Empty
from multiprocessing import Pool, Queue, JoinableQueue
from datetime import datetime
from bs4 import BeautifulSoup

"""
This class is responsible for collecting and counting
tags in one domain.

Todo:
    Add persistence for tag_count.
"""


class TagCounter:

    def __init__(self, domain, input_col):
        self.domain = domain
        self.input_col = input_col
        self.htmls_queue = JoinableQueue()
        self.tags_queue = Queue()
        self.tag_count = {}

    def count_tags(self):
        tag_count_start_time = datetime.now()
        self.fill_htmls_queue()

        print("Input htmls_queue size: ", self.htmls_queue.qsize())

        pool = Pool(settings.NUM_PROCESSORS, self.processor)
        self.htmls_queue.join()

        print("Output tags_queue size: ", self.tags_queue.qsize())

        tags_merge_start_time = datetime.now()
        while True:  # TODO: move tags merging to separete process (>20% processing time)
            try:
                tags_batch = self.tags_queue.get(True, 2)
            except Empty:
                print("No batches in tags_queue. tags_queue size: %d htmls_queue size: %d" %
                      (self.tags_queue.qsize(), self.htmls_queue.qsize()))
                break

            for tag, count in tags_batch.items():
                if self.tag_count.get(tag):
                    self.tag_count[tag] += count
                else:
                    self.tag_count[tag] = count

        finish_time = datetime.now()
        print("Tags merging time: %s" % (finish_time - tags_merge_start_time))

        print("Tags counting time: %s" % (finish_time - tag_count_start_time))

        pool.close()  # TODO: why if we are closing pool before tags are merged tags_queue.get is getting stucked (?)

    # def persist_tags(): saves to pickle file or Mongo

    def fill_htmls_queue(self):
        domain_htmls = list(self.input_col.find({'domain': self.domain}))  # TODO: get slices from mongo

        # TODO: check amount of tags in slowly processed HTMLs
        batch_size = settings.BATCH_SIZE

        html_batch_start_time = datetime.now()

        while domain_htmls:
            batch = domain_htmls[:batch_size]
            self.htmls_queue.put(batch)  # TODO: move html_batching to separete process
            domain_htmls = domain_htmls[batch_size:]

        print("HTMLs batching time: %s" % (datetime.now() - html_batch_start_time))

    def processor(self):
        pages_processed = 0
        pid = os.getpid()

        while True:
            tag_count = {}

            try:
                htmls_batch = self.htmls_queue.get(True, 2)  # TODO: test changing timeout
            except Empty:
                print("No batches in htmls_queue, pid: %d tags_queue size: %d htmls_queue size: %d" %
                      (pid, self.tags_queue.qsize(), self.htmls_queue.qsize()))  # TODO: check if htmls_queue still exist at that moment
                break

            print("Process %d created, batch len: %d" % (pid, len(htmls_batch)))

            for page in htmls_batch:

                if page.get('no_repeat_html'):
                    soup = BeautifulSoup(page['no_repeat_html'], 'html.parser')
                else:
                    soup = BeautifulSoup(page['full_page_html'], 'html.parser')

                for tag in soup.find_all():
                    stag = str(tag).strip().replace(" ", "").lower()  # TODO: add tag processing here as a separate function

                    if stag in tag_count:
                        tag_count[stag] += 1
                    else:
                        tag_count[stag] = 1

                pages_processed += 1

                if pages_processed % 100 == 0:
                    print('%s, %s, pages_processed = %d, len(tag_count) = %d' %
                          (pid, str(datetime.now()), pages_processed, len(tag_count)))

            self.tags_queue.put(tag_count)
            self.htmls_queue.task_done()

    def sort_tags(self):
        self.tag_count = sorted(self.tag_count.items(), key=operator.itemgetter(1), reverse=True)
