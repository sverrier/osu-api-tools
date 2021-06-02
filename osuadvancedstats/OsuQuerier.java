package osuadvancedstats;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;


public class OsuQuerier {
	public static void yearlyQueries(int start, int end) {
		
		try {
			
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres",
					"root");
			Statement s = connection.createStatement();
			String query = "COPY (select * from (select tophundred.user_id, tophundred.beatmap_id, date_played, "
					+ "ROW_NUMBER() OVER(partition by tophundred.beatmap_id order by score desc) as beatmap_rank "
					+ "from tophundred inner join beatmaps on tophundred.beatmap_id = beatmaps.beatmap_id) a "
					+ "where a.beatmap_rank = 1 order by date_played asc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\oldestTop1s.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			for(int i = start; i <= end; i++) {
				System.out.println(i);
				query = "COPY (select username, sum(score) as ranked_score "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id inner join users on maxscore.user_id = users.user_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "group by username order by ranked_score desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\rankedscore_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " score");
				
				query = "COPY (select username, (weighted_pp(pp_index, max_pp) + bonus_pp(count(pp_index))) as weighted_pp "
						+ "from (select maxscore.user_id, maxscore.beatmap_id, max(maxscore.pp) as max_pp, "
						+ "ROW_NUMBER() OVER(partition by maxscore.user_id order by max(maxscore.pp) desc) as pp_index "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "group by maxscore.user_id, maxscore.beatmap_id) as a inner join users on a.user_id = users.user_id "
						+ "group by username order by weighted_pp desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\weightedpp_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " weighted pp");
				
				query = "COPY (select username, count(*) as fc_count from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and (maxcombo - combo) <= count100 and countmiss = 0) as a inner join users on a.user_id = users.user_id"
						+ " group by username order by fc_count desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\fc_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " fc count");
				
				query = "COPY (select username, count(*) as top_1s from (select tophundred.user_id, tophundred.beatmap_id, "
						+ "ROW_NUMBER() OVER(partition by tophundred.beatmap_id order by score desc) as beatmap_rank "
						+ "from tophundred inner join beatmaps on tophundred.beatmap_id = beatmaps.beatmap_id where approved_date "
						+ "between '" + i + "-01-01 00:00:01' and '" + i +"-12-31 23:59:59') as a inner join users on a.user_id = users.user_id "
						+ "where a.beatmap_rank = 1 group by username order by top_1s desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\top1_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " #1s");
				
				query = "COPY (select username, count(*) as top_50s from (select tophundred.user_id, tophundred.beatmap_id, "
						+ "ROW_NUMBER() OVER(partition by tophundred.beatmap_id order by score desc) as beatmap_rank "
						+ "from tophundred inner join beatmaps on tophundred.beatmap_id = beatmaps.beatmap_id where approved_date "
						+ "between '" + i + "-01-01 00:00:01' and '" + i +"-12-31 23:59:59') as a inner join users on a.user_id = users.user_id "
						+ "where a.beatmap_rank <= 50 group by username order by top_50s desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\top50_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " top 50s");
				
				query = "COPY (select username, sum(max_pp) as total_pp from (select user_id, maxscore.beatmap_id, "
						+ "max(pp) as max_pp from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "group by user_id, maxscore.beatmap_id) as a inner join users on a.user_id = users.user_id "
						+ "group by username order by total_pp desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\totalpp_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " total pp");
				
				query = "COPY (select username, count(*) as clears from (select user_id "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59') as a "
						+ "inner join users on a.user_id = users.user_id group by username order by clears desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\clears_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " clears");
				
				query = "COPY (select username, count(*) as total_ss from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and rank like 'X%') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by total_ss desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\totalss_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " total ss");
				
				query = "COPY (select username, count(*) as total_s from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and rank like 'S%') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by total_s desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\totals_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " total s");
				
				query = "COPY (select username, count(*) as silver_ss from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and rank = 'XH') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by silver_ss desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\ssh_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " silver ss");
				
				query = "COPY (select username, count(*) as silver_s from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and rank = 'SH') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by silver_s desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\sh_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " silver s");
				
				query = "COPY (select username, count(*) as gold_ss from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and rank = 'X') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by gold_ss desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\goldss_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " gold ss");
				
				query = "COPY (select username, count(*) as gold_s from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and rank = 'S') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by gold_s desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\golds_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " gold s");
				
				query = "COPY (select username, count(*) as a_ranks from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id"
						+ " where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "and rank = 'A') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by a_ranks desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\a-rank_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(i + " a ranks");
				
				
			
			}
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
		
	}
	
	public static void globalRankings() {
		try {
			
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres",
					"root");
			Statement s = connection.createStatement();
			
			/*String query = "COPY (select username, score from (select user_id, sum(scorezero) as score from "
					+ "(select user_id, set_id,  max(score) as scorezero from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "group by user_id, set_id) s group by user_id) t inner join users on t.user_id = users.user_id order by score desc) "
					+ "TO 'C:\\\\Users\\\\sensa\\\\Documents\\\\VSCode\\\\bot\\\\data\\\\scorev0.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("scorev0");*/
			
			String query = "COPY (select username, pow(score, 0.5) as score from (select user_id, sum(pow(score, 2)) as score from maxscore group by user_id) a "
			+ "inner join users on a.user_id = users.user_id order by score desc limit 10000) "
			+ "TO 'C:\\\\Users\\\\sensa\\\\Documents\\\\VSCode\\\\bot\\\\data\\\\scoresquared.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("score rms");
			
			query = "COPY (select username, (weighted_pp(score_index, score) + bonus_pp(count(score_index))) as weighted_score "
					+ "from (select maxscore.user_id, maxscore.beatmap_id, score, "
					+ "ROW_NUMBER() OVER(partition by user_id order by score desc) as score_index "
					+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "group by user_id, maxscore.beatmap_id) as a inner join users on a.user_id = users.user_id "
					+ "group by username order by weighted_score desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\weightedscore.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("weighted score");
			
			double values[] = {0.90, 0.95, 0.97, 0.98, 0.99, 0.995, 0.999};
			for(double v : values) {
				query = "COPY (select username, (weighted_pp(pp_index, max_pp, " + v + ") + bonus_pp(count(pp_index))) as weighted_pp "
						+ "from (select maxscore.user_id, maxscore.beatmap_id, max(maxscore.pp) as max_pp, "
						+ "ROW_NUMBER() OVER(partition by maxscore.user_id order by max(maxscore.pp) desc) as pp_index "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "group by maxscore.user_id, maxscore.beatmap_id) as a inner join users on a.user_id = users.user_id "
						+ "group by username order by weighted_pp desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\weightedpp_" + v + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println("weighted pp");
			}
			
			query = "COPY (select username, fc_score from (select user_id, sum(score) as fc_score "
					+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "where (maxcombo - combo) <= count100 and countmiss = 0 and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%') "
					+ "group by user_id) a inner join users on a.user_id = users.user_id "
					+ " order by fc_score desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\fcscore.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("fc score");
			
			query = "COPY (select username, sum(score) as ss_score "
					+ "from maxscore inner join users on maxscore.user_id = users.user_id "
					+ "where rank like '%X%' and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%') "
					+ "group by username order by ss_score desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\ssscore.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("ss score");
			
			query = "COPY (select username, count(*) as top_1s from (select tophundred.user_id, tophundred.beatmap_id, "
					+ "ROW_NUMBER() OVER(partition by tophundred.beatmap_id order by score desc) as beatmap_rank "
					+ "from tophundred inner join beatmaps on tophundred.beatmap_id = beatmaps.beatmap_id) a inner join users on a.user_id = users.user_id "
					+ "where a.beatmap_rank = 1 group by username order by top_1s desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\top1_full.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("#1s");
			
			query = "COPY (select username, count(*) as top_50s from (select tophundred.user_id, tophundred.beatmap_id, "
					+ "ROW_NUMBER() OVER(partition by tophundred.beatmap_id order by score desc) as beatmap_rank "
					+ "from tophundred inner join beatmaps on tophundred.beatmap_id = beatmaps.beatmap_id) a inner join users on a.user_id = users.user_id "
					+ "where a.beatmap_rank <= 50 group by username order by top_50s desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\top50_full.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("top 50s");
			
			query = "COPY (select username, sum(max_pp) as total_pp from (select user_id, maxscore.beatmap_id, "
					+ "max(pp) as max_pp from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "group by user_id, maxscore.beatmap_id) as a inner join users on a.user_id = users.user_id "
					+ "group by username order by total_pp desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\totalpp_full.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("total pp");
			
			query = "Copy (select username, ROUND((weighted_pp(final.pp_index, final.pp) + bonus_pp(count(pp_index))), 2) as weighted_pp "
					+ "from (select user_ID, pp, ROW_NUMBER() OVER(partition by user_id order by pp desc) as pp_index "
					+ "from (select user_id, beatmap_id, true_pp_classic(diffcalc(stars, position), timemult(days), statusmult(rank, perfect), "
					+ "modmult(enabled_mods), popularitymult(playcount), ssmult(ss_count, unique_plays), "
					+ "accmult(count300, count100, count50, countmiss), 1, 1) as pp "
					+ "from (select user_id, beatmaps.beatmap_id, cast(DATE_PART('day', NOW() - date_played) as int) as days, "
					+ "LEAST(stars, 5) as stars, enabled_mods, rank, perfect, ss_count, playcount, passcount / 10 as unique_plays, "
					+ "cast(ROW_NUMBER() OVER(partition by beatmaps.beatmap_id order by score desc) as int) as position, "
					+ "count300, count100, count50, countmiss from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "inner join ss_count on beatmaps.beatmap_id = ss_count.beatmap_id) source) inter) final "
					+ "inner join users on final.user_id = users.user_id group by username order by weighted_pp desc ) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\ppv1truefull.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("ppv1 classic");
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	public static void weeklyQueries(String start, String end, int week) {
		try {			
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			
			Statement s = connection.createStatement();
			String query = "COPY (select username, sum(score) as ranked_score "
					+ "from (select user_id, osualternative.beatmap_id, score "
					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
					+ "where approved_date between '" +  start + "' and '" + end + "' ) as a "
					+ "inner join users on a.user_id = users.user_id "
					+ "group by username order by ranked_score desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" +  "rankedscore_" + week + ".csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("weekly score");
			query = "COPY (select username, count(*) as fcs "
					+ "from (select user_id, osualternative.beatmap_id "
					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
					+ "where approved_date between '" +  start + "' and '" + end + "' "
					+ "and (maxcombo - combo) <= count100 and countmiss = 0 and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%')) as a "
					+ " inner join users on a.user_id = users.user_id "
					+ "group by username order by fcs desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" +  "fccount_" + week + ".csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("weekly fc");
			query = "COPY (select username, count(*) as ss "
					+ "from (select user_id, osualternative.beatmap_id "
					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
					+ "where approved_date between '" +  start + "' and '" + end + "' "
					+ "and rank like 'X%') as a "
					+ "inner join users on a.user_id = users.user_id "
					+ "group by username order by ss desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" +  "sscount_" + week + ".csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("weekly ss");
			query = "COPY (select username, count(*) as clears "
					+ "from (select user_id, osualternative.beatmap_id "
					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
					+ "where approved_date between '" +  start + "' and '" + end + "' "
					+ "and enabled_mods not in (select value from mods where name like '%NF%')) as a inner join users on a.user_id = users.user_id "
					+ "group by username order by clears desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" +  "clearcount_" + week + ".csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("weekly clears");
			
			query = "COPY (select username, count(*) as clears "
					+ "from (select user_id, osualternative.beatmap_id "
					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
					+ "where approved_date between '" +  start + "' and '" + end + "' "
					+ ") as a inner join users on a.user_id = users.user_id "
					+ "group by username order by clears desc limit 5000) "
					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" +  "playcount_" + week + ".csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("weekly playss");
			
			
			s.close();
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void yearToDateQueries() {
    	
    	try {
    		Class.forName("org.postgresql.Driver");
    		Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
    		
    		Statement s = connection.createStatement();
        	
        	Hashtable<String, ArrayList<Double>> diffs = new Hashtable<>();
    		ArrayList<Double> d = new ArrayList<>();
    		d.add(0.00);
    		d.add(20.00);
    		diffs.put("all", d);
    		
    		d = new ArrayList<>();
    		d.add(0.00);
    		d.add(2.00);
    		diffs.put("easy", d);
    			
    		d = new ArrayList<>();
    		d.add(2.00);
    		d.add(2.80);
    		diffs.put("normal", d);
    		
    		d = new ArrayList<>();
    		d.add(2.80);
    		d.add(4.00);
    		diffs.put("hard", d);
    		
    		d = new ArrayList<>();
    		d.add(4.00);
    		d.add(5.30);
    		diffs.put("insane", d);
    		
    		d = new ArrayList<>();
    		d.add(5.30);
    		d.add(6.50);
    		diffs.put("extra", d);
    		
    		d = new ArrayList<>();
    		d.add(6.50);
    		d.add(20.00);
    		diffs.put("extreme", d);
    		
    		for(String key : diffs.keySet()) {
    			double lower = diffs.get(key).get(0);
    			double higher = diffs.get(key).get(1);
    			String query = "COPY (select username, sum(score) as ranked_score "
    					+ "from (select user_id, osualternative.beatmap_id, score "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + ") as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by ranked_score desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "score_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly score " + key);
    			
    			query = "COPY (select username, (weighted_pp(a.pp_index, a.pp) + bonus_pp(count(pp_index))) as weighted_pp "
    					+ "from (select user_id, osualternative.beatmap_id, pp, "
    					+ "ROW_NUMBER() OVER(partition by user_id order by pp desc) as pp_index "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ ") as a inner join users on a.user_id = users.user_id "
    					+ "group by username order by weighted_pp desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" + "pp_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly pp" + key);
    			
    			query = "COPY (select username, count(*) as fcs "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and (maxcombo - combo) <= count100 and countmiss = 0 and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%')) as a "
    					+ " inner join users on a.user_id = users.user_id "
    					+ "group by username order by fcs desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "fc_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly fc " + key);
    			
    			query = "COPY (select username, count(*) as clears "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and enabled_mods not in (select value from mods where name like '%NF%')) as a inner join users on a.user_id = users.user_id "
    					+ "group by username order by clears desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "clears_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly clears " + key);
    			
    			query = "COPY (select username, count(*) as plays "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ ") as a inner join users on a.user_id = users.user_id "
    					+ "group by username order by plays desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "plays_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly plays " + key);
    			
    			query = "COPY (select username, count(*) as ss "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank like 'X%') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by ss desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "ss_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly ss " + key);
    			
    			query = "COPY (select username, count(*) as s "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank like 'S%') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by s desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "s_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly s " + key);
    			
    			query = "COPY (select username, count(*) as top_1s from (select osualternative.user_id, osualternative.beatmap_id, "
    					+ "ROW_NUMBER() OVER(partition by osualternative.beatmap_id order by score desc) as beatmap_rank "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + ") "
    					+ "as a inner join users on a.user_id = users.user_id "
    					+ "where a.beatmap_rank = 1 group by username order by top_1s desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" + "top1_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly #1  " + key);
    			
    			query = "COPY (select username, count(*) as top_50s from (select osualternative.user_id, osualternative.beatmap_id, "
    					+ "ROW_NUMBER() OVER(partition by osualternative.beatmap_id order by score desc) as beatmap_rank "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + ") "
    					+ "as a inner join users on a.user_id = users.user_id "
    					+ "where a.beatmap_rank <= 50 group by username order by top_50s desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" + "top50_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly #1  " + key);
    			
    			query = "COPY (select username, count(*) as silver_ss "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'XH') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by silver_ss desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "silver_ss_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly silver ss " + key);
    			
    			query = "COPY (select username, count(*) as silver_s "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'SH') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by silver_s desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "silver_s_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly silver s " + key);
    			
    			query = "COPY (select username, count(*) as gold_ss "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'X') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by gold_ss desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "gold_ss_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly gold ss " + key);
    			
    			query = "COPY (select username, count(*) as gold_s "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'S') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by gold_s desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "gold_s_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly gold s " + key);
    			
    			query = "COPY (select username, count(*) as a_ranks "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'A') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by a_ranks desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "a_ranks_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly a ranks " + key);
    			
    			query = "COPY (select username, count(*) as b_ranks "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'B') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by b_ranks desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "b_ranks_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly b ranks " + key);
    			
    			query = "COPY (select username, count(*) as c_ranks "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'C') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by c_ranks desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "c_ranks_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly c ranks " + key);
    			
    			query = "COPY (select username, count(*) as d_ranks "
    					+ "from (select user_id, osualternative.beatmap_id "
    					+ "from osualternative inner join beatmaps on osualternative.beatmap_id = beatmaps.beatmap_id "
    					+ "where approved_date >= '2021-01-01 00:00:00' and stars >= " + lower + " and stars < " + higher + " "
    					+ "and rank = 'D') as a "
    					+ "inner join users on a.user_id = users.user_id "
    					+ "group by username order by d_ranks desc limit 5000) "
    					+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\2021_" +  "d_ranks_" + key + ".csv' DELIMITER ',' CSV HEADER;";
    			s.execute(query);
    			System.out.println("yearly d ranks " + key);
    		}
    		
    		s.close();
			connection.close();
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    }
	
	public static void miscalleneousQueries() {
		try {
			
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			Statement s = connection.createStatement();
			
			String query = "insert into fc_count (select beatmaps.beatmap_id, coalesce(count(*), 0) as fc_count "
					+ "from beatmaps left outer join maxscore on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "where (maxcombo - combo) <= count100 and countmiss = 0 and enabled_mods not in "
					+ "(select value from mods where name like '%HT%' or name like '%EZ%') "
					+ "group by beatmaps.beatmap_id) ON CONFLICT (beatmap_id) DO UPDATE SET fc_count = excluded.fc_count";
			s.execute(query);
			System.out.println("fc count");
			
			
			query = "insert into ss_count (select beatmaps.beatmap_id, coalesce(count(*), 0) as ss_count "
					+ "from beatmaps left outer join maxscore on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "where rank like '%X%' and enabled_mods not in "
					+ "(select value from mods where name like '%HT%' or name like '%EZ%') "
					+ "group by beatmaps.beatmap_id) ON CONFLICT (beatmap_id) DO UPDATE SET ss_count = excluded.ss_count";
			s.execute(query);
			System.out.println("ss count");
			
			query = "Insert into first_fc (select b.beatmap_id, user_id, DATE_PART('day', date_played - b.approved_date) as difference "
					+ "from (select user_id, beatmap_id, date_played, approved_date, ROW_NUMBER() OVER(partition by beatmap_id order by date_played asc) as date_rank "
					+ "from (select user_id, beatmaps.beatmap_id, date_played, approved_date from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
					+ "where (maxcombo - combo) <= count100 and countmiss = 0 and enabled_mods not in "
					+ "(select value from mods where name like '%HT%' or name like '%EZ%')) as fc) as b where date_rank = 1) ON CONFLICT DO NOTHING";
			s.execute(query);
			System.out.println("first fc");
			
			query = "Insert into first_ss (select b.beatmap_id, user_id, DATE_PART('day', date_played - approved_date) as difference "
					+ "from (select user_id, beatmap_id, date_played, ROW_NUMBER() OVER(partition by beatmap_id order by date_played asc) as date_rank "
					+ "from (select user_id, beatmap_id, date_played from scores where rank like '%X%' and enabled_mods not in "
					+ "(select value from mods where name like '%HT%' or name like '%EZ%')) as ss) as b "
					+ "inner join beatmaps on b.beatmap_id = beatmaps.beatmap_id where date_rank = 1) ON CONFLICT DO NOTHING";
			s.execute(query);
			System.out.println("first ss");
			
			query = "COPY (select username, count(*) from maxscore inner join users on users.user_id = maxscore.user_id "
					+ "where maxscore.count100 = 1 and maxscore.countmiss = 0 and maxscore.count50 = 0 group by username order by count(*) desc) "
					+ "TO 'C:\\\\Users\\\\sensa\\\\Documents\\\\VSCode\\\\bot\\\\data\\\\1x100.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("1x100");
			query = "COPY (select username, count(*) from maxscore inner join users on users.user_id = maxscore.user_id "
					+ "where maxscore.count100 = 0 and maxscore.countmiss = 0 and maxscore.count50 = 1 group by username order by count(*) desc) "
					+ "TO 'C:\\\\Users\\\\sensa\\\\Documents\\\\VSCode\\\\bot\\\\data\\\\1x50.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("1x50");
			query = "COPY (select username, count(*) from maxscore inner join users on users.user_id = maxscore.user_id "
					+ "where maxscore.count100 = 0 and maxscore.countmiss = 1 and maxscore.count50 = 0 group by username order by count(*) desc) "
					+ "TO 'C:\\\\Users\\\\sensa\\\\Documents\\\\VSCode\\\\bot\\\\data\\\\1xmiss.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("1x0");
			query = "COPY (select username, count(*) from maxscore inner join users on users.user_id = maxscore.user_id "
					+ "where maxscore.countmiss = 1 group by username order by count(*) desc) "
					+ "TO 'C:\\\\Users\\\\sensa\\\\Documents\\\\VSCode\\\\bot\\\\data\\\\onemiss.csv' DELIMITER ',' CSV HEADER;";
			s.execute(query);
			System.out.println("1 miss");
			
			
			
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	public static void bonusQueries() {
		try {
			
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			Statement s = connection.createStatement();
			String query = "";
			Hashtable<String, ArrayList<Double>> diffs = new Hashtable<>();
			ArrayList<Double> d = new ArrayList<>();
			d.add(0.00);
			d.add(20.00);
			diffs.put("all", d);
			
			d = new ArrayList<>();
			d.add(0.00);
			d.add(2.00);
			diffs.put("easy", d);
			
			d = new ArrayList<>();
			d.add(2.00);
			d.add(2.80);
			diffs.put("normal", d);
			
			d = new ArrayList<>();
			d.add(2.80);
			d.add(4.00);
			diffs.put("hard", d);
			
			d = new ArrayList<>();
			d.add(4.00);
			d.add(5.30);
			diffs.put("insane", d);
			
			d = new ArrayList<>();
			d.add(5.30);
			d.add(6.50);
			diffs.put("extra", d);
			
			d = new ArrayList<>();
			d.add(6.50);
			d.add(20.00);
			diffs.put("extreme", d);
			
			d = new ArrayList<>();
			d.add(0.00);
			d.add(2.00);
			diffs.put("one", d);
			
			d = new ArrayList<>();
			d.add(2.00);
			d.add(3.00);
			diffs.put("two", d);
			
			d = new ArrayList<>();
			d.add(3.00);
			d.add(4.00);
			diffs.put("three", d);
			
			d = new ArrayList<>();
			d.add(4.00);
			d.add(5.00);
			diffs.put("four", d);
			
			d = new ArrayList<>();
			d.add(5.00);
			d.add(6.00);
			diffs.put("five", d);
			
			d = new ArrayList<>();
			d.add(6.00);
			d.add(7.00);
			diffs.put("six", d);
			
			d = new ArrayList<>();
			d.add(7.00);
			d.add(8.00);
			diffs.put("seven", d);
			
			d = new ArrayList<>();
			d.add(8.00);
			d.add(9.00);
			diffs.put("eight", d);
			
			d = new ArrayList<>();
			d.add(9.00);
			d.add(10.00);
			diffs.put("nine", d);
			
			d = new ArrayList<>();
			d.add(10.00);
			d.add(20.00);
			diffs.put("ten", d);
			
			for(String key : diffs.keySet()) {
				double lower = diffs.get(key).get(0);
				double higher = diffs.get(key).get(1);
				
				query = "COPY (select username, sum(score) as ranked_score "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id inner join users on maxscore.user_id = users.user_id "
						+ "where stars >= " + lower + " and stars < " + higher + " "
						+ "group by username order by ranked_score desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\rankedscore_" + key + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(key + " score");
				
				query = "COPY (select username, (weighted_pp(pp_index, pp) + bonus_pp(count(pp_index))) as weighted_pp "
						+ "from (select maxscore.user_id, maxscore.beatmap_id, pp, "
						+ "ROW_NUMBER() OVER(partition by maxscore.user_id order by pp desc) as pp_index "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where stars >= " + lower + " and stars < " + higher + " "
						+ ") as a inner join users on a.user_id = users.user_id "
						+ "group by username order by weighted_pp desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\weightedpp_" + key + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println("weighted pp");
				
				query = "COPY (select username, count(*) as fc_count from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where stars >= " + lower + " and stars < " + higher + " "
						+ "and (maxcombo - combo) <= count100 and countmiss = 0 and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%')) as a inner join users on a.user_id = users.user_id"
						+ " group by username order by fc_count desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\fc_" + key + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(key + " fc");
				
				query = "COPY (select username, count(*) as total_ss from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where stars >= " + lower + " and stars < " + higher + " "
						+ "and rank like 'X%') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by total_ss desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\totalss_" + key + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(key + " total ss");
				
				query = "COPY (select username, count(*) as clears from (select user_id "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where stars >= " + lower + " and stars < " + higher + ") as a "
						+ "inner join users on a.user_id = users.user_id group by username order by clears desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\clears_" + key + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(key + " clears");
			
				query = "COPY (select sum(max_score) from (select max(score) as max_score from tophundred inner join beatmaps on tophundred.beatmap_id = beatmaps.beatmap_id "
						+ "where stars >= " + lower + " and stars < " + higher + " "
						+ "group by beatmaps.beatmap_id) as a) TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\maxscore_" + key + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println("maxscore " + key);
				
				query = "COPY (select sum(max_score) from (select max(score) as max_score from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where stars >= " + lower + " and stars < " + higher + " and enabled_mods = '0' "
						+ "group by beatmaps.beatmap_id) as a) TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\nomodscore_" + key + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println("nomodscore " + key);
			}
			
			for(int i = 2021; i <= 2021; i++) {
				query = "COPY (select sum(max_score) from (select max(score) as max_score from tophundred inner join beatmaps on tophundred.beatmap_id = beatmaps.beatmap_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "group by beatmaps.beatmap_id) as a) TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\maxscore_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println("maxscore " + i);
				
				query = "COPY (select sum(max_score) from (select max(score) as max_score from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' and enabled_mods = '0' "
						+ "group by beatmaps.beatmap_id) as a) TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\nomodscore_" + i + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println("nomodscore " + i);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getLocalizedMessage());
		}

	}
	
	public static void bonusProjects() {
		try {
			Class.forName("org.postgresql.Driver");
	        Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
	        Statement s = connection.createStatement();
	        
	        ArrayList<String> entries = new ArrayList<>();
	        entries.add("%hitogata%ryuusei%");
	        entries.add("%sword%art%online%");
	        entries.add("%demetori%");
	        entries.add("%dragonforce%");
	        entries.add("%touhou%");
	        for(String entry : entries) {
		        String query = "COPY (select username, count(*) as total_fc from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where LOWER(source || ' ' || tags || ' ' || artist || ' ' || title || ' ' || creator) like '" + entry + "' and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%')"
						+ "and (maxcombo - combo) <= count100 and countmiss = 0) as a inner join users on a.user_id = users.user_id"
						+ " group by username order by total_fc desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\fc_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
		        s.execute(query);
				System.out.println(entry + " fc");
				
				query = "COPY (select username, count(*) as total_ss from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where LOWER(source || ' ' || tags || ' ' || artist || ' ' || title || ' ' || creator) like '" + entry + "' and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%')"
						+ "and rank like 'X%') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by total_ss desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\ss_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(entry + " total ss");
				
				query = "COPY (select username, sum(score) as ranked_score "
						+ "from (select user_id, beatmaps.beatmap_id, score "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where LOWER(source || ' ' || tags || ' ' || artist || ' ' || title || ' ' || creator) like '" + entry + "') as a "
						+ "inner join users on a.user_id = users.user_id "
						+ "group by username order by ranked_score desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" +  "score_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(entry + " score");
				query = "COPY (select username, count(*) as clears from (select user_id "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where LOWER(source || ' ' || tags || ' ' || artist || ' ' || title || ' ' || creator) like '" + entry + "') as a "
						+ "inner join users on a.user_id = users.user_id group by username order by clears desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\clears_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(entry + " clears");
	        }
	        
	        entries.clear();
	        entries.add("5.00");
	        entries.add("6.00");
	        for(String entry : entries) {
	        	String query = "COPY (select username, count(*) as total_ss from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where round(stars, 2) = " + entry + " and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%')"
						+ "and rank like 'X%') as a inner join users on a.user_id = users.user_id"
						+ " group by username order by total_ss desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\ss_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(entry + " total ss");
			    query = "COPY (select username, count(*) as fc_count from "
						+ "(select user_id from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where round(stars, 2) = " + entry + " "
						+ "and (maxcombo - combo) <= count100 and countmiss = 0 "
						+ "and enabled_mods not in (select value from mods where name like '%HT%' or name like '%EZ%')) "
						+ "as a inner join users on a.user_id = users.user_id "
						+ "group by username order by fc_count desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\fc_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(entry + " fc");
				query = "COPY (select username, sum(score) as ranked_score "
						+ "from (select user_id, beatmaps.beatmap_id, score "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where round(stars, 2) = " + entry + ") as a "
						+ "inner join users on a.user_id = users.user_id "
						+ "group by username order by ranked_score desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\" +  "score_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(entry + " score");
				query = "COPY (select username, count(*) as clears from (select user_id "
						+ "from maxscore inner join beatmaps on maxscore.beatmap_id = beatmaps.beatmap_id "
						+ "where round(stars, 2) = " + entry + ") as a "
						+ "inner join users on a.user_id = users.user_id group by username order by clears desc limit 5000) "
						+ "TO 'C:\\Users\\sensa\\Documents\\VSCode\\bot\\data\\clears_" + entry + ".csv' DELIMITER ',' CSV HEADER;";
				s.execute(query);
				System.out.println(entry + " clears");
	        }
			connection.close();
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
		
	}
	
	public static void createtables() {
		try {
			Class.forName("org.postgresql.Driver");
	        Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
	        Statement s = connection.createStatement();
	        
	        String query = "insert into fc_by_sr (select user_id, round(stars, 2) as difficulty, count(*) as fc_count from "
	        		+ "(select * from maxscore where user_id in (select * from priorityuser)) a inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id "
	        		+ "where countmiss = 0 and count100 >= (maxcombo - combo) and enabled_mods not in "
	        		+ "(select value from mods where name like '%EZ%' or name like '%HT%') group by difficulty, user_id) "
	        		+ "on conflict (user_id, stars) do update set fc = EXCLUDED.fc";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("fc");
	        
	        query = "insert into ss_by_sr (select user_id, round(stars, 2) as difficulty, count(*) as ss_count from "
	        		+ "(select * from maxscore where user_id in (select * from priorityuser) and rank like '%X%' ) a "
	        		+ "inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id group by difficulty, user_id) "
	        		+ "on conflict (user_id, stars) do update set ss = EXCLUDED.ss";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("ss");
	        
	        query = "Insert into clears_by_sr (select user_id, round(stars, 2) as difficulty, count(*) as clear_count from "
	        		+ "(select * from maxscore where user_id in (select * from priorityuser)) a "
	        		+ "inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id group by difficulty, user_id) "
	        		+ "on conflict (user_id, stars) do update set clears = EXCLUDED.clears";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("clears");
	        
	        query = "Insert into score_by_sr (select user_id, round(stars, 2) as difficulty, sum(score) as score_amount from "
	        		+ "(select * from maxscore where user_id in (select * from priorityuser)) a "
	        		+ "inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id group by difficulty, user_id) "
	        		+ "on conflict (user_id, stars) do update set score = EXCLUDED.score;";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("score");
	        
	        
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	public static void updateCompletionists() {
		try {
			Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            Statement s = connection.createStatement();
            s.executeUpdate("delete from completionist");
            s.executeUpdate("insert into completionist select beatmaps.beatmap_id, user_id, "
            		+ "case when enabled_mods in (select value from mods where name like '%NF%') then 'NF' "
            		+ "when enabled_mods in (select value from mods where name like '%HT%') then 'HT' "
            		+ "when enabled_mods in (select value from mods where name like '%EZ%') then 'EZ' "
            		+ "when countmiss > 0 then 'Miss' when countmiss = 0 and (maxcombo - combo) > count100 then 'Sliderbreak' "
            		+ "when (maxcombo - combo) <= count100 and (count100 > 0 or count50 > 0) then 'FC' else 'SS' end as status "
           			+ "from beatmaps inner join (select * from maxscore where user_id in (select * from alpha_user)) a on beatmaps.beatmap_id = a.beatmap_id where mode = 0");
            System.out.println("completionist");
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	public static void updateAll() {
		try {
			Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            Statement s = connection.createStatement();
            s.executeUpdate("insert into registeredscore select * from maxscore where user_id in (select * from priorityuser) "
            		+ "on conflict on constraint registeredscore_pkey do update set score = excluded.score, count300 = EXCLUDED.count300, " 
            		+ "count100 = EXCLUDED.count100, count50 = EXCLUDED.count50, countmiss = EXCLUDED.countmiss, combo = EXCLUDED.combo, "
            		+ "perfect = EXCLUDED.perfect, enabled_mods = EXCLUDED.enabled_mods, date_played = EXCLUDED.date_played, rank = EXCLUDED.rank, "
            		+ "pp = EXCLUDED.pp, replay_available = EXCLUDED.replay_available where EXCLUDED.score > registeredscore.score;");
            s.executeUpdate("delete from registered");
            s.executeUpdate("insert into registered select beatmaps.beatmap_id, user_id, "
            		+ "case when enabled_mods in (select value from mods where name like '%NF%') then 'NF' "
            		+ "when enabled_mods in (select value from mods where name like '%HT%') then 'HT' "
            		+ "when enabled_mods in (select value from mods where name like '%EZ%') then 'EZ' "
            		+ "when countmiss > 0 then 'Miss' when countmiss = 0 and (maxcombo - combo) > count100 then 'Sliderbreak' "
            		+ "when (maxcombo - combo) <= count100 and (count100 > 0 or count50 > 0) then 'FC' else 'SS' end as status "
           			+ "from beatmaps inner join registeredscore on beatmaps.beatmap_id = registeredscore.beatmap_id where mode = 0");
            System.out.println("completionist");
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	
}
