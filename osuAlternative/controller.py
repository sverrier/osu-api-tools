import discord
import formatter

def checkProfile(token, cur, di):
    #format the base level data
    base = ("select username, " + token + " as stat from users")
    base = base + buildWhereClause(di)
    
    #build and execute the leaderboard creating query
    query = buildLeaderboard(base, di)
    embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))
    cur.execute(query)

    s = "```diff\n" + formatter.leaderboard(cur)
    if(di.__contains__("-u")):
        user = str(di["-u"]).replace("+", " ").lower()
        query = buildLeaderboard(base, di, user)
        cur.execute(query)
        s += formatter.leaderboard(cur)
    embed.description = s + "```"

    return embed

def checkMappers(token, cur, di):
    #format the base level data
    base = ("select username, count(distinct " + token + ") as stat from beatmaps inner join users on user_id = creator_id group by username")
    base = base + buildWhereClause(di)
    
    #build and execute the leaderboard creating query
    query = buildLeaderboard(base, di)
    embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))
    cur.execute(query)

    s = "```diff\n" + formatter.leaderboard(cur)
    if(di.__contains__("-u")):
        user = str(di["-u"]).replace("+", " ").lower()
        query = buildLeaderboard(base, di, user)
        cur.execute(query)
        s += formatter.leaderboard(cur)
    embed.description = s + "```"

    return embed

def checkBeatmaps(cur, di):
    operation = "count(*)"
    limit = 10
    page = 1
    if di.__contains__("-l"):
        limit = di["-l"]
    if di.__contains__("-p"):
        page = di["-p"]
    offset = int(limit) * (int(page) - 1)

    if di.__contains__("-o"):
        if di["-o"] == "length":
            operation = "sum(length)"
    if not di.__contains__("-mode"):
        di["-mode"] = 0
        
    query = "select " + operation + " from beatmaps"
    query = query + buildWhereClause(di)
    cur.execute(query)
    ans = cur.fetchone()[0]
    if operation == "sum(length)":
        days = ans//(3600*24)
        hours = (ans // 3600) % 24
        minutes = (ans // 60) % 60
        return "Length: " + str(days) + "d" + str(hours) + "h" + str(minutes) + "m"
    if operation == "count(*)":
        return "Count: " + str(ans)
    return ans
    #else:
        #embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))
        #query = "select set_id, beatmaps.beatmap_id, artist, title, diffname, round(stars, 2) as stars from beatmaps where mode = 0"
        #query = query + " and round(stars, 2) >= " + str(mini) + " and round(stars, 2) < " + str(maxi)
        #query = query + " and approved_date >= " + str(start) + " and approved_date < " + str(end) + " order by stars limit " + str(limit) + " offset " + str(offset) 
        #cur.execute(query)
        #s = "" 
        #for b in cur.fetchall():
        #    s = s + str(b[5])[0:4] + "★ | " + "[" + b[2] + " - " + b[3] + " [" + b[4] + "]](https://osu.ppy.sh/beatmapsets/" + str(b[0]) + "#osu/" + str(b[1]) + ")\n"
        #embed.description = s
        #return embed

def checkfile(filename, di):
    limit = 10
    page = 1
    if di.__contains__("-l"):
        limit = di["-l"]
    if di.__contains__("-p"):
        page = di["-p"]
    offset = int(limit) * (int(page) - 1)
        
    f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" + filename + ".csv", newline='')
    embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))
    s = formatter.formatcsv(f, int(limit), int(offset))

    if di.__contains__("-u"):
        f = open("C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" + filename + ".csv", newline='')
        di["-u"] = str(di["-u"]).replace("+", " ")
        s = s[:-3] + formatter.finduser(f, di["-u"])
    embed.description = s

    return embed

def checkTables(operation, table, cur, di):
    base = "select username, " + str(operation) + " as stat from " + str(table) + " inner join users on " + str(table) + ".user_id = users.user_id"

    base = base + buildWhereClause(di)
    base = base + " group by username"
    query = buildLeaderboard(base, di)
    cur.execute(query)

    embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))

    s = "```diff\n" + formatter.leaderboard(cur)
    if(di.__contains__("-u")):
        user = str(di["-u"]).replace("+", " ").lower()
        query = buildLeaderboard(base, di, user)
        cur.execute(query)
        s += formatter.leaderboard(cur) 
    embed.description = s + "```"
    return embed

def getBeatmapList(di, cur, tables=None, sets=False):
    limit = 10
    page = 1
    order = "stars"
    direction = "asc"
    if di.__contains__("-order"):
        order = di["-order"]
    if di.__contains__("-direction"):
        direction = di["-direction"]
    if di.__contains__("-l"):
        limit = di["-l"]
    if di.__contains__("-p"):
        page = di["-p"]
    offset = int(limit) * (int(page) - 1)

    if not di.__contains__("-mode"):
        di["-mode"] = "0"

    if di.__contains__("-u"):
        if not (di["-u"]).isnumeric():
            di["-u"] = str(di["-u"]).replace("+", " ").lower()
            cur.execute("select user_id from users where LOWER(username) = '" + str(di["-u"]).lower() + "'")
            di["-u"] = cur.fetchone()[0]
        di["-user"] = di["-u"]



    query = "select set_id, beatmaps.beatmap_id, artist, title, diffname, round(stars, 2) as stars from beatmaps"
    if sets:
        query = "select set_id, max(beatmap_id) as beatmap_id, max(artist) as artist, max(title) as title, max(diffname) as diffname, max(stars) as stars, max(rating) as rating from beatmaps"
    if tables != None:
        for table in tables:
            query = query + " inner join " + table + " on beatmaps.beatmap_id = " + table + ".beatmap_id"
    query = query + buildWhereClause(di)
    if sets:
        query = query + " group by set_id"
    query = query + " order by " + order + " " + direction + ", artist limit " + str(limit) + " offset " + str(offset)
    cur.execute(query)

    embed = discord.Embed(title = 'Result', colour=discord.Colour(0xE5E242))

    s = "" 
    for b in cur.fetchall():
        s = s + str(b[5])[0:4] + "★ | " + "[" + b[2] + " - " + b[3] + " [" + b[4] + "]](https://osu.ppy.sh/beatmapsets/" + str(b[0]) + "#osu/" + str(b[1]) + ")\n"
    embed.description = s

    return embed


def buildWhereClause(di):
    where = ""
    if di.__contains__("-min"):
        where = where + " and ROUND(stars, 2) >= " + str(di["-min"])
    if di.__contains__("-max"):
        where = where + " and ROUND(stars, 2) < " + str(di["-max"])
    if di.__contains__("-time"):
        where = where + " and days >= " + str(di["-time"])
    if di.__contains__("-start"):
        where = where + " and approved_date >= '" + str(di["-start"]) + "'"
    if di.__contains__("-end"):
        where = where + " and approved_date < '" + str(di["-end"]) + "'"
    if di.__contains__("-mode"):
        where = where + " and mode = " + str(di["-mode"])
    if di.__contains__("-year"):
        where = where + " and approved_date >= '" + str(di["-year"]) + "-01-01 00:00:00' and approved_date <= '" + str(di["-year"]) + "-12-31 23:59:59'"
    if di.__contains__("-status"):
        where = where + " and LOWER(status) = '" + str(di["-status"]).lower() + "'"
    if di.__contains__("-user") and not di.__contains__("-unplayed"):
        where = where + " and user_id = '" + str(di["-user"]) + "'"
    if di.__contains__("-country"):
        where = where + " and country = '" + str(di["-country"]) + "'"
    if di.__contains__("-score"):
        where = where + " and ranked_score > " + str(di["-score"])
    if di.__contains__("-fc"):
        where = where + " and fc_count > 0 and fc_count <= " + str(di["-fc"])
    if di.__contains__("-ss"):
        where = where + " and ss_count > 0 and ss_count <= " + str(di["-fc"])
    if di.__contains__("-unplayed"):
        where = where + " and beatmaps.beatmap_id not in (select beatmap_id from registered where user_id = " + str(di["-user"]) + ")"
    if where != "":
        where = " where" + where[4:]
    
    return where
    

def buildLeaderboard(base, di, user=None):
    limit = 10
    page = 1
    
    if di.__contains__("-l"):
        limit = di["-l"]
    if di.__contains__("-p"):
        page = di["-p"]

    offset = int(limit) * (int(page) - 1)

    rank = "select username, stat, ROW_NUMBER() OVER(order by stat desc) as rank from (" + base + ") base"
    data = "select rank, username, stat from (" + rank + ") r order by rank"

    if user != None:
        query = "select * from (" + data + ") data where LOWER(username) = '" + user + "'"
    else:
        query = "select * from (" + data + ") data limit " + str(limit) + " offset " + str(offset)

    return query