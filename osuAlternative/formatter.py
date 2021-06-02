import csv

def formatcsv(f, limit, offset=0):
    s = "```diff\n"
    with f as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for index, row in enumerate(spamreader):
            if index <= int(offset):
                continue
            if index == offset + limit + 1:
                break
            fixed_user = "{0:<15}".format(row[0])
            fixed_index = "{0:<3}".format(str(index))
            fixed_number = f'{int(float(row[1])):,}'
            s = s + "#" + fixed_index + " | " + fixed_user + " | " + fixed_number + '\n'
    return s + "```"

def leaderboard(cursor):
    s = ""
    for index, row in enumerate(cursor.fetchall()):
        fixed_user = "{0:<15}".format(str(row[1]))
        fixed_rank = "{0:<3}".format(str(row[0]))
        fixed_number = f'{int(float(row[2])):,}'
        s = s + "#" + fixed_rank + " | " + fixed_user + " | " + fixed_number + '\n'
    return s

def tocsv(cursor):
    s = ""
    for index, row in enumerate(cursor.fetchall()):
        for entry in row:
            s = s + str(entry) + ", "
        s = s + "\n"
    return s

def finduser(f, user):
    s = ""
    user = user.replace("+", " ")
    with f as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for index, row in enumerate(spamreader):
            if row[0].lower() == user.lower():
                fixed_user = "{0:<15}".format(row[0])
                fixed_index = "{0:<3}".format(str(index))
                fixed_number = f'{int(float(row[1])):,}'
                s = s + "#" + fixed_index + " | " + fixed_user + " | " + fixed_number + '\n'
        if s == "":
            s += "\n"
        return s + "```"