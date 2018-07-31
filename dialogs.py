def delete_dialog(len_tags_for_del, repeating_tags_count, tag, count):
    print("\n\n===================================================================================")
    # len_tags_for_del = len(tags_for_deleting)
    print("Repeating tags: %d Tags processed: %d\n\n" % (repeating_tags_count - len_tags_for_del, len_tags_for_del))
    delete = input('\nTag: \n\n %s \n\nRepeats: %d times \n'
                   'Do you want to delete it? \n'
                   '["y" + enter] - yes\n'
                   '["r" + enter] - check previous tag\n'
                   '[enter] - no\n'
                   '["stop" + enter] - stop processing tags for this domain \n' % (tag, count))

    return delete

def undo_dialog(tag, choice):
    print("\n\n===================================================================================")
    undo = input('previous choice for tag:\n\n %s \n\n'
                 'Delete = %s\n'
                 'Do you want to change it?\n'
                 '["y" + enter] - set to True\n'
                 '["n" + enter] - set to False\n'
                 '[enter or "r" + enter] - go to previous tag\n' % (tag, choice))

    return undo

def add_to_cleaned_dialog(domain):
    add_to_cleaned = input('\n\n\nDo you want to add domain "%s" to cleaned?(Y/n)\n' % domain)

    return add_to_cleaned

def quit_dialog():
    quit_choice = input('\n\n==================================================================================='
                        '\nDo you want to quit?(Y/n)\n')

    return quit_choice