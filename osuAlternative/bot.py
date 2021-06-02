import discord
import datetime
import threading
import csv
import time
import asyncio
import psycopg2
import formatter
import controller
import documentation
from discord.ext import commands

client = commands.Bot(command_prefix = '!')

client.remove_command('help')

diffs = {}
diffs["easy"] = [0, 2]
diffs["normal"] = [2, 2.8]
diffs["hard"] = [2.8, 4]
diffs["insane"] = [4, 5.3]
diffs["extra"] = [5.3, 6.5]
diffs["extreme"] = [6.5, 20]
diffs["all"] = [0, 20]

starts = ["2021-01-01 00:00:00", "2021-01-08 00:00:00", "2021-01-15 00:00:00", "2021-01-22 00:00:00", "2021-01-29 00:00:00", "2021-02-05 00:00:00", "2021-02-12 00:00:00", "2021-02-19 00:00:00", "2021-02-26", "2021-03-05", "2021-03-12", "2021-03-19", "2021-03-26", "2021-04-02", "2021-04-09", "2021-04-16", "2021-04-23", "2021-04-30", "2021-05-07", "2021-05-14", "2021-05-21", "2021-05-28", "2021-06-04"]

conn = psycopg2.connect("dbname=osu user=bot password=root")
conn.set_session(autocommit=True)
cur = conn.cursor()

today = datetime.datetime.today().strftime('%Y-%m-%d')
lastmonth = (datetime.datetime.today() - datetime.timedelta(days=29)).strftime('%Y-%m-%d')
lastweek = (datetime.datetime.today() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')

@client.event 
async def on_ready():
    print('Ready')

@client.event
async def on_member_join(member):
    print(f'{member} just joined the server.')

def getArgs(arg=None):
    args = []
    if arg != None:
        args = arg.split()
    di = {}
    for i in range(0,len(args)//2):
        di.update({args[2*i]:args[2*i+1]})
    return di

@client.command(pass_context=True)
async def help(ctx, arg=None):
    await ctx.send(documentation.help(arg))
    await updatelists()
    
async def updateweeklies(week):
    channel = client.get_channel(792818934743040052)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\rankedscore_" + str(week) + ".csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if (str(week) + "/52 of project 2021") in str(fetchMessage[0].content):
        await fetchMessage[0].edit(content = "**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))
    else:
        await channel.send("**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))

    channel = client.get_channel(792819496829452290)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\fccount_" + str(week) + ".csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if (str(week) + "/52 of project 2021") in str(fetchMessage[0].content):
        await fetchMessage[0].edit(content = "**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))
    else:
        await channel.send("**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))

    channel = client.get_channel(792819520581664768)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\sscount_" + str(week) + ".csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if (str(week) + "/52 of project 2021") in str(fetchMessage[0].content):
        await fetchMessage[0].edit(content = "**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))
    else:
        await channel.send("**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))

    channel = client.get_channel(792819540567916574)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\clearcount_" + str(week) + ".csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if (str(week) + "/52 of project 2021") in str(fetchMessage[0].content):
        await fetchMessage[0].edit(content = "**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))
    else:
        await channel.send("**Week " + str(week) + "/52 of project 2021" + "**\n" + formatter.formatcsv(f, 50))

async def updateyearlies():
    channel = client.get_channel(792863236705746954)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_score_all.csv", newline='')  
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

    channel = client.get_channel(792863272860123156)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_fc_all.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

    channel = client.get_channel(792863301184913469)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_ss_all.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

    channel = client.get_channel(792863357563961364)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_clears_all.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

    channel = client.get_channel(795159066133004308)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_pp_all.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))
    print("update complete")

    channel = client.get_channel(795159261755736104)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_top1_all.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))
    print("update complete")

    channel = client.get_channel(795159304024883230)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_top50_all.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))
    print("update complete")

async def updateplayers():
    channel = client.get_channel(792875271782531102)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\totalhits.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

    channel = client.get_channel(792883515565146112)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\totalscore.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

    channel = client.get_channel(792883547559952415)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\playcount.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

    channel = client.get_channel(792920423011844106)
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\playtime.csv", newline='')
    fetchMessage = await channel.history(limit = 1).flatten()
    if len(fetchMessage) == 0:
        await channel.send('hello')
        fetchMessage = await channel.history(limit = 1).flatten()
    await fetchMessage[0].edit(content = formatter.formatcsv(f, 50))

async def updatelists():
    channel = client.get_channel(793570054008340511)
    cur.execute("select * from newfcs")
    li = cur.fetchall()
    if len(li) > 0:
        for entry in li:
            beatmap = entry[0]
            user = entry[1]
            date = entry[2]
            cur.execute("select artist, title, diffname, approved_date, set_id, stars, length, maxcombo, cs, ar, od, hp from beatmaps where beatmap_id = " + str(beatmap))
            b = cur.fetchone()
            approved_date = b[3]
            minutes = b[6] // 60
            seconds = b[6] % 60

            differential = ((date.year * 365) + (date.month*31) + (date.day)) - ((approved_date.year * 365) + (approved_date.month*31) + (approved_date.day))

            if(differential >= 7):
                cur.execute("select username from users where user_id = " + str(user))
                result = cur.fetchone()
                username = user
                if result is not None:
                    username = result[0]
                embed = discord.Embed(title = 'A map has been FCed for the first time after ' + str(differential) + ' days!', colour=discord.Colour(0xE5E242))
                s = ""
                s += "**Player: **" + str(username) + "\n"
                s += "**Map: **[" + b[0] + " - " + b[1] + " [" + b[2] + "]](https://osu.ppy.sh/beatmapsets/" + str(b[4]) + "#osu/" + str(beatmap) + ")\n"
                s += "**Time of play: **" + str(date) + "\n"
                s += "**Date ranked: **" + str(approved_date) + "\n\n"
                s += "**Beatmap information**\n"
                s += "CS **" + str(b[8])[0:3] + "** • AR **" + str(b[9])[0:3] + "** • OD **" + str(b[10])[0:3] + "** • HP **" + str(b[11])[0:3] + "** • **" + str(b[5])[0:4] + "★**" + "\n"
                s += "**" + str(minutes) + "m" + str(seconds) + "s** • **" + str(b[7]) + " combo**\n"
                embed.description = s
                await channel.send(embed=embed)

            cur.execute("delete from newfcs where beatmap_id = " + str(beatmap))
        cur.execute("COMMIT;")

    channel = client.get_channel(793594664262303814)
    cur.execute("select * from newSSs")
    li = cur.fetchall()
    if len(li) > 0:
        for entry in li:
            beatmap = entry[0]
            user = entry[1]
            date = entry[2]
            cur.execute("select artist, title, diffname, approved_date, set_id, stars, length, maxcombo, cs, ar, od, hp from beatmaps where beatmap_id = " + str(beatmap))
            b = cur.fetchone()
            approved_date = b[3]
            minutes = b[6] // 60
            seconds = b[6] % 60

            differential = ((date.year * 365) + (date.month*31) + (date.day)) - ((approved_date.year * 365) + (approved_date.month*31) + (approved_date.day))

            if(differential >= 30):
                cur.execute("select username from users where user_id = " + str(user))
                result = cur.fetchone()
                username = user
                if result is not None:
                    username = result[0]
                embed = discord.Embed(title = 'A map has been SSed for the first time after ' + str(differential) + ' days!', colour=discord.Colour(0xE5E242))
                s = ""
                s += "**Player: **" + str(username) + "\n"
                s += "**Map: **[" + b[0] + " - " + b[1] + " [" + b[2] + "]](https://osu.ppy.sh/beatmapsets/" + str(b[4]) + "#osu/" + str(beatmap) + ")\n"
                s += "**Time of play: **" + str(date) + "\n"
                s += "**Date ranked: **" + str(approved_date) + "\n\n"
                s += "**Beatmap information**\n"
                s += "CS **" + str(b[8])[0:3] + "** • AR **" + str(b[9])[0:3] + "** • OD **" + str(b[10])[0:3] + "** • HP **" + str(b[11])[0:3] + "** • **" + str(b[5])[0:4] + "★**" + "\n"
                s += "**" + str(minutes) + "m" + str(seconds) + "s** • **" + str(b[7]) + " combo**\n"
                embed.description = s
                await channel.send(embed=embed)

            cur.execute("delete from newSSs where beatmap_id = " + str(beatmap))
        cur.execute("COMMIT;")

@client.command(pass_context=True)
async def update(ctx):
    t = int(time.time())
    for i in range(0, 52):
        if (t > (1609459200 + (i * 7 * 24 * 60 * 60) + (24*60*60))):
            week = i + 1
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    lastmonth = (datetime.datetime.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    print(lastmonth)
    await updateweeklies(week)
    await updateyearlies()
    await updateplayers()
    await updatelists()

@client.command(pass_context=True)
async def register(ctx, arg):
    arg = str(arg.replace("+", " "))
    if not arg.isnumeric():
        cur.execute("select user_id from users where LOWER(username) = '" + str(arg).lower() + "'")
        user_id = cur.fetchone()[0]
    else:
        user_id = arg
    print(user_id)
    cur.execute("insert into priorityuser values (" + str(user_id) + ") ON CONFLICT DO NOTHING;")
    cur.execute("COMMIT;")
    await ctx.send("Registered!")
    await update()

@client.command(pass_context=True)
async def queue(ctx, *, arg=None):
    args = []
    year = None
    if arg != None:
        args = arg.split()
    di = {}
    for i in range(0,len(args)//2):
        di.update({args[2*i]:args[2*i+1]})
    if di.__contains__("-min") and di.__contains__("-max") and di.__contains__("-start") and di.__contains__("-end") and di.__contains__("-u") :
        if not (di["-u"]).isnumeric():
            cur.execute("select user_id from users where LOWER(username) = '" + str(di["-u"]).lower() + "'")
            user_id = cur.fetchone()[0]
        else:
            user_id = di["-u"]
        optimized = "1"
        if di.__contains__("-optimized"):
            optimized = str(di["-optimized"])
        cur.execute("insert into queue values (" + str(user_id) + ", " + str(di['-min']) + ", " + str(di['-max']) + ", '" + str(di['-start']) + "', '" + str(di['-end']) + "', " + optimized + ")")
        cur.execute("COMMIT;")
        await ctx.send("Queued!")
    else:
        await ctx.send("Malformed command")

@client.command(pass_context=True)
async def scorequeue(ctx, *, arg=None):
    args = []
    year = None
    if arg != None:
        args = arg.split()
    di = {}
    for i in range(0,len(args)//2):
        di.update({args[2*i]:args[2*i+1]})
    if not (di["-u"]).isnumeric():
        cur.execute("select user_id from users where LOWER(username) = '" + str(di["-u"]).lower() + "'")
        user_id = cur.fetchone()[0]
    else:
        user_id = di["-u"]

    cur.execute("insert into scorequeue values (" + str(user_id) + ", " + str(di["-b"]) + ")")
    cur.execute("COMMIT;")
    await ctx.send("Queued!")

@client.command(pass_context=True)
async def getfile(ctx, *, arg=None):
    args = []
    if arg != None:
        args = arg.split()
    di = {}
    for i in range(0,len(args)//2):
        di.update({args[2*i]:args[2*i+1]})
    if di["-type"] == "neverbeenssed":
        cur.execute("select set_id, beatmaps.beatmap_id, artist, title, diffname, round(stars, 2) as stars from neverbeenssed inner join beatmaps on neverbeenssed.beatmap_id = beatmaps.beatmap_id order by stars, artist")
    elif di["-type"] == "neverbeenfced":
        cur.execute("select set_id, beatmaps.beatmap_id, artist, title, diffname, round(stars, 2) as stars from neverbeenfced inner join beatmaps on neverbeenfced.beatmap_id = beatmaps.beatmap_id order by stars, artist")
    elif di["-type"] == "registered":
        cur.execute("select * from priorityuser")
    s = formatter.tocsv(cur)
    f = open("tmp.txt", "w", encoding="utf-8")
    f.write(s)

    f = open("tmp.txt", "r", encoding="utf-8")

    with open("tmp.txt", "rb") as file:
        await ctx.send("Your file is:", file=discord.File(file, str(di["-type"]) + ".csv"))


@client.command(pass_context=True)
async def firstmap(ctx, *, arg=None):
    t = int(time.time())
    for i in range(0, 52):
        if (t > (1609459200 + (i * 7 * 24 * 60 * 60) + (24*3600))):
            week = i + 1

    args = []
    if arg != None:
        args = arg.split()
    di = {}
    for i in range(0,len(args)//2):
        di.update({args[2*i]:args[2*i+1]})
    if di.__contains__("-w"):
        week = di["-w"]

    start = starts[int(week) - 1]

    cur.execute("select artist, title, set_id from beatmaps where approved_date >= '" + str(start) + "' order by approved_date limit 1")
    row = cur.fetchone()
    s = "The first map for week " + str(week) + " is: https://osu.ppy.sh/beatmapsets/" + str(row[2])
    await ctx.send(s)
    await updatelists()

@client.command(pass_context=True)
async def projectXYZ(ctx, *, arg=None):
    args = []
    if arg != None:
        args = arg.split()
    di = {}
    for i in range(0,len(args)//2):
        di.update({args[2*i]:args[2*i+1]})
    if not di.__contains__("-v"):
        await ctx.send("Please specify a value using -v")
    if di.__contains__("-o"):
        if di["-o"] in ["fc", "score", "clears", "ss"]:
            f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" + di["-o"] + "_" + str(di["-v"]) + ".csv", newline='')
            embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))
            s = formatter.formatcsv(f, 10)

            if di.__contains__("-u"):
                di["-u"] = str(di["-u"]).replace("+", " ")
                f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" + di["-o"] + "_" + str(di["-v"]) + ".csv", newline='')
                s = s[:-3] + formatter.finduser(f, di["-u"])
            embed.description = s
            await ctx.send(embed=embed)
        else:
            await ctx.send("Not implemented yet")
    else:
        await ctx.send("Please specify a leaderboard type using -o")


@client.command(pass_context=True)
async def beatmaps(ctx, *, arg=None):
    di = getArgs(arg)
    ans = controller.checkBeatmaps(cur, di)
    await ctx.send(ans)
    await updatelists()

@client.command(pass_context=True)
async def getscores(ctx, *, arg=None):
    di = getArgs(arg)
    
    if not (di["-u"]).isnumeric():
        user = str(di["-u"]).replace("+", " ").lower()
        cur.execute("select user_id from users where LOWER(username) = '" + user + "'")
        user_id = cur.fetchone()[0]
    else:
        user_id = di["-u"]

    di["-user"] = user_id

    if di.__contains__("-o") and di["-o"] == "count":
        query = "select status, count(*) from registered inner join beatmaps on registered.beatmap_id = beatmaps.beatmap_id"
        query = query + controller.buildWhereClause(di)
        query = query + " group by status order by status"
        cur.execute(query)
        s = ""
        for row in cur.fetchall():
            s = s + "status = " + str(row[0]) + ": " + str(row[1]) + "\n"
        await ctx.send(s)
    elif di.__contains__("-o") and di["-o"] == "unplayed":
        di["-unplayed"] = "yes"
        embed = controller.getBeatmapList(di, cur)
        await ctx.send(embed=embed)
    else:
        embed = controller.getBeatmapList(di, cur, ["registered", "fc_count", "ss_count"])
        await ctx.send(embed=embed)

@client.command(pass_context=True)
async def beatmapsets(ctx, *, arg=None):
    args = []
    year = None
    if arg != None:
        args = arg.split()
    di = {}
    for i in range(0,len(args)//2):
        di.update({args[2*i]:args[2*i+1]})
    if di.__contains__("-y"):
        year = di["-y"]

    query = "select count(*) from (select distinct set_id from beatmaps where mode = 0"
    if year != None:
        query = query + " and approved_date >= '" + year + "-01-01 00:00:00' and approved_date <= '" + year + "-12-31 23:59:59'"
    query = query + ") as a"
    cur.execute(query)
    await ctx.send("There are currently " + str(cur.fetchone()[0]) + " ranked beatmap sets that match those parameters")
    await updatelists()

@client.command(pass_context=True)
async def stats(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-u"):
        await ctx.send("Please specify a user using -u")
        return
    if not di.__contains__("-o"):
        await ctx.send("Please specify a leaderboard option using -o")
        return
    if di["-o"] == "score":
        board = "rankedscore"
    elif di["-o"] == "fc":
        board = "fccount"
    elif di["-o"] == "ss":
        board = "sscount"
    elif di["-o"] == "clears":
        board = "clearcount"
    elif di["-o"] == "plays":
        board = "playcount"
    else:
        await ctx.send("Please specify a valid leaderboard type using -o")
        return
    user = str(di["-u"]).replace("+", " ")
    s = "```\nStats for user " + user + "\n```"
    t = int(time.time())
    for i in range(0, 52):
        if (t > (1609459200 + (i * 7 * 24 * 60 * 60))):
            week = i + 1
    for i in range(1, week + 1):
        f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" + str(board) + "_" + str(i) + ".csv", newline='')
        s = s[:-3] + "Week " +  str(i) + ": " + formatter.finduser(f, user)

    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" + di["-o"] + "_all.csv", newline='')
    s = s[:-3] + "\nYear to date: " + formatter.finduser(f, user)

    await ctx.send(s)
    await updatelists()

    


@client.command(pass_context=True)
async def maxscore(ctx, *, arg=None):
    di = getArgs(arg)
    param = "all"
    if di.__contains__("-w"):
        week = int(di["-w"])
        start = starts[week - 1]
        end = starts[week]
        cur.execute("select sum(maxscore) from (select max(score) as maxscore from osualternative where beatmap_id in (select beatmap_id from beatmaps where approved_date between '" +  start + "' and '" + end + "') group by beatmap_id) as a")
        await ctx.send("The current max achievable score in week " + str(week) + " is " + str(f'{int(cur.fetchone()[0]):,}'))
    else:
        if di.__contains__("-filter"):
            param = str(di["-filter"])
        f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\maxscore_" + param + ".csv", newline='')
        with f as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for index, row in enumerate(spamreader):
                if index == 0:
                    continue
                await ctx.send("Max score: " + str(f'{int(row[0]):,}'))
    
    await updatelists()

@client.command(pass_context=True)
async def nomodscore(ctx, *, arg=None):
    di = getArgs(arg)
    param = "all"
    if di.__contains__("-filter"):
        param = di["-filter"]

    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\nomodscore_" + param + ".csv", newline='')
    with f as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for index, row in enumerate(spamreader):
            if index == 0:
                continue
            await ctx.send("Max nomod score: " + str(f'{int(row[0]):,}'))

    await updatelists()

@client.command(pass_context=True)
async def weekly(ctx, *, arg=None):
    t = int(time.time())
    for i in range(0, 52):
        if (t > (1609459200 + (i * 7 * 24 * 60 * 60) + (24*3600))):
            week = i + 1
    di = getArgs(arg)

    if di.__contains__("-o"):
        if di["-o"] == "score":
            board = "rankedscore"
        elif di["-o"] == "fc":
            board = "fccount"
        elif di["-o"] == "ss":
            board = "sscount"
        elif di["-o"] == "clears":
            board = "clearcount"
        elif di["-o"] == "plays":
            board = "playcount"
        else:
            await ctx.send("Please specify a valid leaderboard type using -o")
            return
        
        if di.__contains__("-w"):
            week = di["-w"]

        embed = controller.checkfile(str(board) + "_" + str(week), di)
        await ctx.send(embed=embed)
    else:
        await ctx.send("Please specify a leaderboard type using -o")
    await updatelists()


@client.command(pass_context=True)
async def yeartodate(ctx, *, arg=None):
    di = getArgs(arg)
    difficulty = "all"

    if di.__contains__("-o"):
        valid = ["score", "fc", "ss", "s", "clears", "plays", "pp", "top1", "top50", "silver_ss", "silver_s", "gold_ss", "gold_s", "a_ranks", "b_ranks", "c_ranks", "d_ranks"]
        if di["-o"] not in valid:
            await ctx.send("Please specify a valid leaderboard type using -o")
            return

        board = di["-o"]

        if board == "d_ranks":
            s = "HALL OF SHAME\n"
        
        if di.__contains__("-d"):
            difficulty = di["-d"]
        
        embed = controller.checkfile("2021_" + str(board) + "_" + str(difficulty), di)
        await ctx.send(embed=embed)
    else:
        await ctx.send("Please specify a leaderboard type using -o")
    await updatelists()

@client.command(pass_context=True)
async def neverbeenssed(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastmonth)
    embed = controller.getBeatmapList(di, cur, ["neverbeenssed"])
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def neverbeenfced(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastweek)
    embed = controller.getBeatmapList(di, cur, ["neverbeenfced"])
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def query(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-o"):
        if di["-o"] in ["fc", "ss", "clears", "score"]:
            embed = controller.checkTables("sum(" + str(di["-o"]) + ")", str(di["-o"]) + "_by_sr", cur, di)
            await ctx.send(embed=embed)
        else:
            await ctx.send("Please specify a valid leaderboard type using -o")

@client.command(pass_context=True)
async def toprated(ctx, *, arg=None):
    di = getArgs(arg)
    di["-order"] = "rating"
    di["-direction"] = "desc"
    embed = controller.getBeatmapList(di, cur, None, True)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def totalhits(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkProfile("(count300 + count100 + count50)", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def playtime(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkProfile("cast(playtime / 3600 as int)", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def playcount(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkProfile("playcount", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def totalscore(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkProfile("total_score", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def rankedscore(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("rankedscore_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("ranked_score", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def clears(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("clears_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("(ssh_count + sh_count + ss_count + s_count + a_count)", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def total_ss(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("totalss_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("(ssh_count + ss_count)", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def total_s(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("totals_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("(sh_count + s_count)", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def silver_s(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("sh_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("sh_count", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def silver_ss(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("ssh_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("ssh_count", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def gold_ss(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("gold_ss_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("ss_count", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def gold_s(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("gold_s_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("s_count", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def a_ranks(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("a-rank_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("a_count", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def pp(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("weightedpp_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        embed = controller.checkProfile("pp_raw", cur, di)
        await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def mapsranked(ctx, arg=None):
    di = getArgs(arg)
    embed = controller.checkMappers("beatmap_id", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def setsranked(ctx, arg=None):
    di = getArgs(arg)
    embed = controller.checkMappers("set_id", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def scoreperclear(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-score"):
        di["-score"] = 1000000000
    print(di["-score"])
    embed = controller.checkProfile("cast(ranked_score / greatest((ssh_count + sh_count + s_count + ss_count + a_count), 1) as int)", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def scoreratio(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-score"):
        di["-score"] = 1000000000
    embed = controller.checkProfile("cast(ranked_score as float) / cast(GREATEST(total_score, 1) as float) * 1000", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def top50s(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("top50_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        await ctx.send("Please specify a filter")
    await updatelists()

@client.command(pass_context=True)
async def top1s(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("top1_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        await ctx.send("Please specify a filter")
    await updatelists()

@client.command(pass_context=True)
async def scorev0(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("scorev0", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def ppv1_unstable(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("ppv1true", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def ppv1_new(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("ppv1full", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def ppv1(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("ppv1truefull", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def weightedscore(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("weightedscore", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def scoresquared(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("scoresquared", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def fcscore(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("fcscore", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def ssscore(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile("ssscore", di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def totalpp(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("totalpp_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        await ctx.send("Please specify a filter")
    await updatelists()

@client.command(pass_context=True)
async def fc_count(ctx, *, arg=None):
    di = getArgs(arg)
    if di.__contains__("-filter"):
        embed = controller.checkfile("fc_" + di["-filter"], di)
        await ctx.send(embed=embed)
    else:
        await ctx.send("Please specify a filter")
    await updatelists()

@client.command(pass_context=True)
async def first_fc(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-time"):
        di["-time"] = 1
    embed = controller.checkTables("count(*)", "first_fc", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def first_ss(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkTables("count(*)", "first_ss", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def unique_fc(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkTables("count(*)", "unique_fc", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def unique_ss(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkTables("count(*)", "unique_ss", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def ss_bounty(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkTables("sum(days)", "first_ss", cur, di)
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def first_fc_list(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastmonth)
    embed = controller.getBeatmapList(di, cur, ["first_fc"])
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def first_ss_list(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastmonth)
    embed = controller.getBeatmapList(di, cur, ["first_ss"])
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def unique_fc_list(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastweek)
    embed = controller.getBeatmapList(di, cur, ["unique_fc"])
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def unique_ss_list(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastmonth)
    embed = controller.getBeatmapList(di, cur, ["unique_ss"])
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def least_fced(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastmonth)
    if not di.__contains__("-order"):
        di["-order"] = "fc_count, stars"
    embed = controller.getBeatmapList(di, cur, ["fc_count"])
    await ctx.send(embed=embed)
    await updatelists()


@client.command(pass_context=True)
async def least_ssed(ctx, *, arg=None):
    di = getArgs(arg)
    if not di.__contains__("-end"):
        di["-end"] = str(lastmonth)
    if not di.__contains__("-order"):
        di["-order"] = "ss_count, stars"
    embed = controller.getBeatmapList(di, cur, ["ss_count"])
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def oldestnumberone(ctx, *, arg=None):
    di = getArgs(arg)

    limit = 10
    page = 1

    if di.__contains__("-l"):
        limit = di["-l"]
    if di.__contains__("-p"):
        page = di["-p"]

    offset = int(limit) * (int(page) - 1)

    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\oldestTop1s.csv", newline='')
    embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))
    s = formatter.formatcsv(f, int(limit), int(offset))
    embed.description = s
    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def tragedy(ctx, *, arg=None):
    di = getArgs(arg)
    if di["-o"] == "x":
        f = "onemiss"
    else:
        f = "1x" + di["-o"]

    embed = controller.checkfile(f, di)

    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def projecthitogata(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile(str(di["-o"]) + "_%hitogata%ryuusei%", di)

    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def projectdemetori(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile(str(di["-o"]) + "_%demetori%", di)

    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def projectdragonforce(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile(str(di["-o"]) + "_%dragonforce%", di)

    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def projectsao(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile(str(di["-o"]) + "_%dragonforce%", di)

    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def projecttouhou(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile(str(di["-o"]) + "_%touhou%", di)

    await ctx.send(embed=embed)
    await updatelists()

@client.command(pass_context=True)
async def pp_fun(ctx, *, arg=None):
    di = getArgs(arg)
    embed = controller.checkfile(di["-o"], di)
    await ctx.send(embed=embed)
    await updatelists()

client.run('NzkyODA1MzUwNjU0NjA3NDMx.X-jDhQ.in_cIsmDMEYJLnyVAxBE63MtyrA')