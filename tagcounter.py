import os

from multiprocessing import Process, Manager
from datetime import datetime
from bs4 import BeautifulSoup


class TagCounter:

    def __init__(self, domain, input_col):
        self.domain = domain
        self.tag_appearance = {}
        self.input_col = input_col

    def create_processors(self, processors_amount):
        domain_htmls = list(self.input_col.find({'domain': self.domain}))

        # TODO: check amount of tags in slowly processed HTMLs
        batch_size = len(domain_htmls) // processors_amount
        manager = Manager()
        tag_batches = []  # TODO: replace with Queue
        processes = []  # TODO: replace with processes pool

        for processor_id in range(processors_amount):
            tag_batches.append(manager.dict())
            if processor_id < processors_amount-1:
                batch = domain_htmls[:batch_size]
            else:
                batch = domain_htmls[:]

            p = Process(target=self.collect_tags, args=(batch, tag_batches[processor_id]))
            processes.append(p)
            p.start()

            domain_htmls = domain_htmls[batch_size:]

        for p in processes:
            p.join()

        print("JOINED")

        for dict in tag_batches:
            print(len(dict))


        self.tag_appearance = {}  # TODO: merge tag_batches

    def collect_tags(self, batch, lines_appearance):
        # choosing and counting tags appearance
        pages_processed = 0
        pid = os.getpid()
        print("Process %d created, batch len: %d" % (pid, len(batch)))

        start_time = datetime.now()
        for page in batch:  # input_col.find({'domain': domain}):  TODO: consume from Queue

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

        print("Time: %s, Process: %d" % (datetime.now() - start_time, pid))

