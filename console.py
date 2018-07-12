import sys
import os

__last_perc = -1
def progress_bar(message, value, endvalue, bar_length = 20):
    global __lastWritten, __last_perc
    ratio = float(value) / endvalue
    percent = int(round(ratio * 100))
    if percent == __last_perc: return
    __last_perc = percent

    arrow = '-' * int(round(ratio * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    text = "\r" + ('   ' + message).ljust(50) + " [{0}] {1}%".format(arrow + spaces, percent)
    if percent == 100: text += '\n'
    sys.stdout.write(text)
    sys.stdout.flush()

def query_yes_no(question, default = "yes"):
    valid = {"yes":True,   "y":True,  "ye":True,
             "no":False,     "n":False}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                             "(or 'y' or 'n').\n")

def query_multiple(question, choices, default = 0):
    while True:
        print(question)
        for (i, choice) in zip(range(len(choices)), choices):
            print(str(i) + ": " + choice)
        sys.stdout.write('Your selection: ')

        choice = input().lower()
        if default is not None and choice == '':
            return default
        elif int(choice) >= 0 and int(choice) < len(choices):
            return int(choice)
        else:
            print("Please respond with a number from 0 to " + str(len(choices) - 1))