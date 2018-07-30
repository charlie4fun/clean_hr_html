import os

from queue import Empty
from multiprocessing import Pool
from datetime import datetime
from bs4 import BeautifulSoup


class TagCounter:

    def __init__(self, domain, input_col, input_queue, output_queue):
        self.domain = domain
        self.tag_appearance = {}
        self.input_col = input_col
        self.input_queue = input_queue
        self.output_queue = output_queue

    def create_processors(self, processors_amount):
        domain_htmls = list(self.input_col.find({'domain': self.domain}))

        # TODO: check amount of tags in slowly processed HTMLs
        batch_size = len(domain_htmls) // processors_amount

        while domain_htmls:
            batch = domain_htmls[:batch_size]
            self.input_queue.put(batch)
            domain_htmls = domain_htmls[batch_size:]



        print(self.input_queue.qsize())

        Pool(processors_amount, self.collect_tags, (self.input_queue, self.output_queue))

        print("try to join")
        self.input_queue.join()

        print("JOINED")


        self.tag_appearance = {}  # TODO: merge tag_batches

    def collect_tags(self, input_queue, output_queue):
        # choosing and counting tags appearance
        lines_appearance = {}
        pages_processed = 0
        pid = os.getpid()

        while True:

            try:
                htmls_batch = input_queue.get(True, 2)
            except Empty:
                print("No batches in input, pid: ", pid, " Output Queue size: ", self.output_queue.qsize(), " Input Queue size: ", self.input_queue.qsize())
                break

            print("Process %d created, batch len: %d" % (pid, len(htmls_batch)))

            start_time = datetime.now()
            for page in htmls_batch:  # TODO: consume from Queue

                if pages_processed % 100 == 0:
                    print('%s, %s, pages_processed = %d, len(lines_appearance) = %d' %
                          (pid, str(datetime.now()), pages_processed, len(lines_appearance)))

                if page.get('no_repeat_html'):
                    soup = BeautifulSoup(page['no_repeat_html'], 'html.parser')
                else:
                    soup = BeautifulSoup(page['full_page_html'], 'html.parser')

                for tag in soup.find_all():
                    stag = str(tag)

                    if stag in lines_appearance:
                        lines_appearance[stag] += 1
                    else:
                        lines_appearance[stag] = 1

                pages_processed += 1

            output_queue.put(sum)

            print("Time: %s, Process: %d" % (datetime.now() - start_time, pid))
