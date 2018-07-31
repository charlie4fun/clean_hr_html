import pymongo
import operator
import sys
import settings

from pymongo.errors import ConnectionFailure
from datetime import datetime
from bs4 import BeautifulSoup
from tagcounter import TagCounter


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

        for domain in partly_clean_domains:
            self.clean_domain(domain, self.partly_clean_col)

            quit_choice = input('\n\n==================================================================================='
                                '\nDo you want to quit?(Y/n)\n')

            if quit_choice in ["y", "Y"]:
                sys.exit()

        for domain in input_domains:
            print(domain)
            self.clean_domain(domain, self.input_col)

            quit_choice = input('\n\n==================================================================================='
                                '\nDo you want to quit?(Y/n)\n')

            if quit_choice in ["y", "Y"]:
                sys.exit()

    def clean_domain(self, domain, input_col):

        tag_counter = TagCounter(domain, input_col)
        tag_counter.count_tags()
        print("Tags count: ", len(tag_counter.tag_count))

        # sys.exit()

        # choosing lines for removing
        repeating_lines = 0
        for count in tag_counter.tag_count.values():
            if count != 1:
                repeating_lines += 1

        lines_for_deleting = []
        tag_counter.tag_count = sorted(tag_counter.tag_count.items(), key=operator.itemgetter(1), reverse=True)
        len_tag_count = len(tag_counter.tag_count)
        i = 0
        while i < len_tag_count:
            line, count = tag_counter.tag_count[i]
            if count != 1:
                position = 0
                # TODO: move all dialogs to separate functions
                print("\n\n===================================================================================")
                len_lines_for_del = len(lines_for_deleting)
                print("Repeating lines: %d Lines processed: %d\n\n" % (repeating_lines-len_lines_for_del, len_lines_for_del))
                delete = input('\nLine: \n\n %s \n\nRepeats: %d times \n'
                               'Do you want to delete it? \n'
                               '["y" + enter] - yes\n'
                               '["r" + enter] - check previous tag\n'
                               '[enter] - no\n'
                               '["stop" + enter] - stop processing tags for this domain \n' % (line, count))

                if delete in ["y", "Y"]:
                    lines_for_deleting.append((line, True))
                    i += 1

                elif delete == "stop":
                    break

                elif delete in ["r", "R"]:
                    while (abs(position-1)<=len(lines_for_deleting)):
                        position -= 1
                        line = lines_for_deleting[position][0]
                        print("\n\n===================================================================================")
                        undo = input('previous choice for tag:\n\n %s \n\n'
                                     'Delete = %s\n'
                                     'Do you want to change it?\n'
                                     '["y" + enter] - set to True\n'
                                     '["n" + enter] - set to False\n'
                                     '[enter or "r" + enter] - go to previous tag\n' % (line, lines_for_deleting[position][1]))

                        if undo in ['y', 'Y']:
                            lines_for_deleting[position] = (line, True)
                            break

                        elif undo in ['n', 'N']:
                            lines_for_deleting[position] = (line, False)
                            break

                    else:
                        print("\n\n===================================================================================")
                        print('No (or no more) previously processed tags')


                else:
                    lines_for_deleting.append((line, False))
                    i += 1
            else:
                i += 1

        # storing tags
        for tag in lines_for_deleting:
            record = {}
            record['tag'] = tag[0]
            record['deleted'] = tag[1]
            record['domain'] = domain
            self.tags_col.insert_one(record)


        # removing repeating tags
        lines_for_deleting = dict(lines_for_deleting)  # TODO: make cleaning parallel
        pages_updated = 0
        for page in input_col.find({'domain': domain}):
            if pages_updated%1000 == 0:
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
                    if lines_for_deleting.get(stag):
                        no_repeat_html=no_repeat_html.replace(stag, ' ')

                page['no_repeat_html'] = no_repeat_html
            self.clean_col.insert_one(page)
            pages_updated +=1

        add_to_cleaned = input('\n\n\nDo you want to add domain "%s" to cleaned?(Y/n)\n' % domain)
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