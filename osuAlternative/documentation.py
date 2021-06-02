def help(arg=None):
    beatmapCommands = ["beatmaps", "neverbeenssed", "neverbeenfced", "least_fced", "queue", "toprated"]
    singleResult = ["beatmaps", "nomodscore", "maxscore", "queue", "scorequeue", "register", "update", "getfile"]
    globalStatCommands = ["totalhits", "playtime", "playcount", "rankedscore", "totalscore", "pp", "clears", "total_ss", "total_s", "silver_ss", "silver_s", "gold_ss", "gold_s", "a_ranks", "scoreratio", "scoreperclear"]
    s = ""
    if arg == None:
        s = s + "More detailed information on what the commands and filters do is available in #info, "
        s = s + "or by requesting help for a specific command using !help command-name.\n\n"
        s = s + "**Global stat commands**: `!totalhits, !playtime, !playcount, !rankedscore, !totalscore, !pp, "
        s = s + "!clears, !total_ss, !total_s, !silver_ss, !silver_s, !gold_ss, !gold_s, !a_ranks, !scorev0, !weightedscore`\n"
        s = s + "Optional: `-p, -l, -country, -filter, -u`\n"
        s = s + "**General stat commands**: `!top50s, !top1s, !fc_count`\n"
        s = s + "Required: `-filter`, Optional: `-p, -l, -country, -u`\n"
        s = s + "**Custom leaderboards**: `!unique_ss, !first_ss, !first_fc`\n"
        s = s + "Optional: `-p, -l, -time, -u`\n"
        s = s + "**Beatmap lists**: `!neverbeenssed, !neverbeenfced, !least_fced`\n"
        s = s + "Optional: `-p, -l, -min, -max, -start, -end, -order`\n"
        s = s + "**Mapper leaderboards**: `!maps_ranked, !sets_ranked`\n"
        s = s + "**Beatmap commands**: `!beatmaps, !beatmapsets, !toprated, !maxscore, !nomodscore`\n"
        s = s + "**Advanced stats**: `!query, !stats, !queue`\n"
        s = s + "**Project 2021**: `!weekly, !yeartodate, !firstmap`\n"
        s = s + "**Miscalleneous**: `!register, !update, !scoreratio, !oldestnumberone, !tragedy, !scorequeue, !getfile`\n\n"
        s = s + "**Parameter explanations**:\n"
        s = s + "`-l`: how many results to output\n"
        s = s + "`-p`: which 'page' in the result to output\n"
        s = s + "`-u`: a specific user to include in the leaderboard\n"
        s = s + "`-o`: leaderboard option in multi-purpose commands\n"
        s = s + "`-order`: the order in which to sort the result\n"
        s = s + "`-direction`: the direction in which to output the results\n"
        s = s + "`-min`: minimal star rating of maps to include (inclusive)\n"
        s = s + "`-max`: maximal star rating of maps to include (exclusive)\n"
        s = s + "`-start`: earliest rank date of maps to include\n"
        s = s + "`-end`: latest rank date of maps to include\n"
        s = s + "`-time`: minimum interval between rank date and play date\n"
        s = s + "`-filter`: year or difficulty range for some leaderboards\n"

    else:
        if arg == "scoreratio":
            s = s + "**Description**: Returns a leaderboard for highest ratio of ranked score to total score.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-score`: Minimum amount of score to be included in the leaderboard. Defaults to 1B\n"
        if arg == "scoreperclear":
            s = s + "**Description**: Returns a leaderboard for the most ranked score per submitted clear\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-score`: Minimum amount of score to be included in the leaderboard. Defaults to 1B\n"
        if arg == "scorev0":
            s = s + "**Description**: Returns a score leaderboard with a cap one 1 play per set, spotlight style.\n\n"
        if arg == "weightedscore":
            s = s + "**Description**: Weighted pp formula, but for score. Weighted score.\n\n"
        if arg == "beatmaps":
            s = s + "**Description**: Returns statistics a set of beatmaps\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-o`: Operation to perform. Defaults to count\n"
            s = s + "\t\t`• count`: Returns the amount of beatmaps that fit the criteria\n"
            s = s + "\t\t`• length`: Returns the total length of the beatmaps that fit the criteria\n"
            s = s + "\t`-mode`: 0 = standard, 1 = taiko, 2 = ctb, 3 = mania. Defaults to 0\n"
        if arg == "toprated":
            s = s + "**Description**: Returns the top rated beatmaps that fit the criteria\n\n"
        if arg == "least_fced":
            s = s + "**Description**: Returns a list of beatmaps ordered by their FC count (starts at 1).\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-fc`: Maximal amount of FCs to be outputted.\n"
        if arg == "ss_bounty":
            s = s + "**Description**: For each first ss a player has, they get the number of days between rank date and play date added to their total.\n\n"
            s = s + "\t`-time`: Minimal amount of interval days to count a given play.\n"
        if arg == "stats":
            s = s + "**Description**: Returns one user's ranking for each week in project 2021.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-u`: User to lookup\n"
            s = s + "\t`-o`: Leaderboard type. ss, fc, clears, plays, score\n"
        if arg == "register":
            s = s + "**Description**: Registers a user for higher priority operations.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-u`: User to register. Username will work if you exist in my larger user pool, otherwise user_id is necessary\n"
        if arg == "maxscore":
            s = s + "**Description**: Outputs the sum of the best play on each map contained in the filter.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-filter`: star rating (one, two, seven, easy, normal, extra, etc) or year\n"
        if arg == "nomodscore":
            s = s + "**Description**: Outputs the sum of the best nomod play on each map contained in the filter.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-filter`: star rating (one, two, seven, easy, normal, extra, etc) or year\n"
        if arg == "weekly":
            s = s + "**Description**: Returns a leaderboard for one week in project 2021.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-o`: Leaderboard type. ss, fc, clears, plays, score\n"
            s = s + "\t`-w`: The week to output. Defaults to this week\n"
            s = s + "\t`-u`: User to lookup (optional)\n"
        if arg == "yeartodate":
            s = s + "**Description**: Returns a leaderboard for the entirety of project 2021.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-o`: Leaderboard type. fc, clears, plays, score, ss, s, silver_s, gold_s, etc\n"
            s = s + "\t`-u`: User to lookup (optional)\n"
            s = s + "\t`-o`: Beatmap diffivulty filter. easy, normal, hard, insane, extra, extreme\n"
        if arg == "getfile":
            s = s + "**Description**: Returns the entire list in a file, if discord allows it.\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-type`: List to fetch. neverbeenssed, neverbeenfced\n"
        if arg == "tragedy":
            s = s + "**Description**: Leaderboard for the most tragedied players\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-o`: 100, 50, miss, x\n"
        if arg == "queue":
            s = s + "**Description**: Queues up a player for a full check of a specified set of beatmaps. Please use extensive parameters to limit the set\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-optimized`: Which beatmaps to ignore\n"
            s = s + "\t\t`• 0`: Ignore no maps (not recommended)\n"
            s = s + "\t\t`• 1`: Ignore beatmaps the player has a registered clear on\n"
            s = s + "\t\t`• 2`: Ignore beatmaps the player has a registered FC on\n"
            s = s + "\t\t`• 3`: Ignore beatmaps the player has an SS on\n"
        if arg == "scorequeue":
            s = s + "**Description**: Queues up a single beatmap for a single player\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-b`: Which beatmap id to check\n"
            s = s + "\t`-u`: Which user id to check\n"
        if arg == "query":
            s = s + "**Description**: Allows for precise star rating filtering on typical leaderboards for registered users\n\n"
            s = s + "**Command parameters**:\n"
            s = s + "\t`-o`: Which stat to query for (fc, ss, clears, score)\n"
            s = s + "\t`-min`: Minimal star rating\n"
            s = s + "\t`-max`: Maximal star rating\n"
        if arg in globalStatCommands:
            s = s + "**Optional parameters**:\n"
            s = s + "\t`-country`: Specify a country using the ISO 2 letter code\n"
            s = s + "\t`-filter`: Specify a year or a difficulty range if applicable\n"
        if arg in beatmapCommands:
            s = s + "**Optional parameters**:\n"
            s = s + "\t`-min`: minimal star rating of maps to include (inclusive)\n"
            s = s + "\t`-max`: maximal star rating of maps to include (exclusive)\n"
            s = s + "\t`-start`: earliest rank date of maps to include\n"
            s = s + "\t`-end`: latest rank date of maps to include\n"
            s = s + "\t`-order`: the field by which to order the results, if applicable\n"
            s = s + "\t`-direction`: the direction which to output the results, if applicable\n"
        if arg not in singleResult:
            s = s + "**Global parameters**:\n"
            s = s + "\t`-p`: Specify the resulting page to output\n"
            s = s + "\t`-l`: Specify how many results to output. Beware the 2000 character limit\n"
    if s == "":
        s = "No documentation."

    return s
