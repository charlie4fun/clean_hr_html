import pymongo
import sys
import settings
import dialogs

from pymongo.errors import ConnectionFailure
from datetime import datetime
from bs4 import BeautifulSoup
from tagcounter import TagCounter

"""
This class implements one of the processing flows for html documents.
"""


class Cleaner:

    def __init__(self):
        client = pymongo.MongoClient(connect=False)
        try:
            result = client.admin.command("ismaster")
        except ConnectionFailure:
            print("Server not available")

        client = pymongo.MongoClient(settings.MONGO_HOST, settings.MONGO_PORT)
        self.db = client[settings.DB_NAME]

        self.input_col = self.db[settings.INPUT_COLLECTION]

        try:
            self.clean_col = self.db.create_collection(settings.CLEAN_COLLECTION)
        except:
            self.clean_col = self.db[settings.CLEAN_COLLECTION]

        try:
            self.partly_clean_col = self.db.create_collection(settings.PARTLY_CLEAN_COLLECTION)
        except:
            self.partly_clean_col = self.db[settings.PARTLY_CLEAN_COLLECTION]

        try:
            self.tags_col = self.db.create_collection(settings.TAGS_COLLECTION)
        except:
            self.tags_col = self.db[settings.TAGS_COLLECTION]

    def clean(self):
        sys.setrecursionlimit(5000)  # TODO: Check the value on my personal computer
        input_domains = self.input_col.distinct('domain')
        partly_clean_domains = self.partly_clean_col.distinct('domain')
        clean_domains = self.clean_col.distinct('domain')

        input_domains = list(set(input_domains)-set(clean_domains)-set(partly_clean_domains))
        # this manipulations made to process partly cleaned domains first
        # and only afterwards in volve new domains
        input_domains = partly_clean_domains + input_domains

        for domain in input_domains:
            print("input_domains: ", input_domains)
            print(domain)
            self.clean_domain(domain, self.input_col)

            quit_choice = dialogs.quit_dialog()

            if quit_choice in ["y", "Y"]:
                sys.exit()

    def clean_domain(self, domain, input_col):

        tag_counter = TagCounter(domain, input_col)
        tag_counter.count_tags()
        print("Distinct tags count: ", len(tag_counter.tag_count))

        # sys.exit()

        # choosing tags for removing
        repeating_tags_count = 0
        for count in tag_counter.tag_count.values():
            if count != 1:
                repeating_tags_count += 1

        tags_for_deleting = []
        tag_counter.sort_tags()
        len_tag_count = len(tag_counter.tag_count)
        i = 0
        while i < len_tag_count:
            tag, count = tag_counter.tag_count[i]
            if count != 1:
                position = 0

                delete = dialogs.delete_dialog(len(tags_for_deleting), repeating_tags_count, tag, count)

                if delete in ["y", "Y"]:
                    tags_for_deleting.append((tag, True))
                    i += 1

                elif delete == "stop":
                    break

                elif delete in ["r", "R"]:
                    while (abs(position-1) <= len(tags_for_deleting)):
                        position -= 1
                        tag = tags_for_deleting[position][0]

                        undo = dialogs.undo_dialog(tag, tags_for_deleting[position][1])

                        if undo in ['y', 'Y']:
                            tags_for_deleting[position] = (tag, True)
                            break

                        elif undo in ['n', 'N']:
                            tags_for_deleting[position] = (tag, False)
                            break

                    else:
                        print("\n\n===================================================================================")
                        print('No (or no more) previously processed tags')

                else:
                    tags_for_deleting.append((tag, False))
                    i += 1
            else:
                i += 1

        # storing tags
        for tag in tags_for_deleting:
            record = {'tag': tag[0], 'deleted': tag[1], 'domain': domain}
            self.tags_col.insert_one(record)

        # removing repeating tags
        tags_for_deleting = dict(tags_for_deleting)  # TODO: make cleaning parallel
        pages_updated = 0
        for page in input_col.find({'domain': domain}):
            if pages_updated % 1000 == 0:
                print('%s, pages_updated = %d' %
                      (str(datetime.now()), pages_updated))

            if page.get('no_repeat_html'):
                soup = BeautifulSoup(page['no_repeat_html'], 'html.parser')
            else:
                soup = BeautifulSoup(page['full_page_html'], 'html.parser')

            all_tags = soup.find_all()
            if all_tags:
                no_repeat_html = str(all_tags[0])

                for tag in all_tags:
                    stag = str(tag)
                    if tags_for_deleting.get(stag):
                        no_repeat_html = no_repeat_html.replace(stag, ' ')

                page['no_repeat_html'] = no_repeat_html
            self.clean_col.insert_one(page)
            pages_updated += 1

        add_to_cleaned = dialogs.add_to_cleaned_dialog(domain)
        self.partly_clean_col.delete_many({'domain': domain})

        # add domain to cleaned or leave for further processing
        if add_to_cleaned in ["y", "Y"]:
            print('Domain "%s" was added to cleaned' % domain)

        else:
            for page in self.clean_col.find({'domain': domain}):
                self.partly_clean_col.insert_one(page)
            self.clean_col.delete_many({'domain': domain})
            print('Domain "%s" was added to PARTLY cleaned' % domain)


if __name__ == "__main__":

    Cleaner().clean()

    # TODO: compare tags from some HTML before and after removing some random tags
