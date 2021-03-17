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
						+ "from (select scores.user_id, scores.beatmap_id, max(scores.pp) as max_pp, "
						+ "ROW_NUMBER() OVER(partition by scores.user_id order by max(scores.pp) desc) as pp_index "
						+ "from scores inner join beatmaps on scores.beatmap_id = beatmaps.beatmap_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "group by scores.user_id, scores.beatmap_id) as a inner join users on a.user_id = users.user_id "
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
				
				query = "COPY (select username, sum(max_pp) as total_pp from (select user_id, scores.beatmap_id, "
						+ "max(pp) as max_pp from scores inner join beatmaps on scores.beatmap_id = beatmaps.beatmap_id "
						+ "where approved_date between '" + i + "-01-01 00:00:00' and '" + i + "-12-31 23:59:59' "
						+ "group by user_id, scores.beatmap_id) as a inner join users on a.user_id = users.user_id "
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
			e.printStackTrace();
		}

	}
	
	public static void bonusProjects() {
		try {
			Class.forName("org.postgresql.Driver");
	        Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
	        Statement s = connection.createStatement();
	        
	        ArrayList<String> entries = new ArrayList<>();
	        entries.add("5.00");
	        //entries.add("6.00");
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
	        		+ "on conflict (user_id, difficulty) do update set fc = EXCLUDED.fc";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("fc");
	        
	        query = "insert into ss_by_sr (select user_id, round(stars, 2) as difficulty, count(*) as ss_count from "
	        		+ "(select * from maxscore where user_id in (select * from priorityuser) and rank like '%X%' ) a "
	        		+ "inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id group by difficulty, user_id) "
	        		+ "on conflict (user_id, difficulty) do update set ss = EXCLUDED.ss";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("ss");
	        
	        query = "Insert into clears_by_sr (select user_id, round(stars, 2) as difficulty, count(*) as clear_count from "
	        		+ "(select * from maxscore where user_id in (select * from priorityuser)) a "
	        		+ "inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id group by difficulty, user_id) "
	        		+ "on conflict (user_id, difficulty) do update set clears = EXCLUDED.clears";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("clears");
	        
	        query = "Insert into score_by_sr (select user_id, round(stars, 2) as difficulty, sum(score) as score_amount from "
	        		+ "(select * from maxscore where user_id in (select * from priorityuser)) a "
	        		+ "inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id group by difficulty, user_id) "
	        		+ "on conflict (user_id, difficulty) do update set score = EXCLUDED.score;";
	        
	        s.executeUpdate(query);
	        
	        System.out.println("score");
	        
	        
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	public static void createscoremax() {
		int counter = 0;
		int total = 0;
		try {
			Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            for(int i = 0; i < 3000000; i += 5000) {
				Statement s = connection.createStatement();
				ResultSet idSet = s.executeQuery(
						"select user_id, beatmap_id, score, count300, count100, count50, countmiss, combo, perfect, enabled_mods, date_played, rank, pp, "
						+ "replay_available from scores where beatmap_id >= " + i + " and beatmap_id < " + (i + 5000));
				Statement s2 = connection.createStatement();

				while (idSet.next()) {

					String user_id = (idSet.getString("user_id"));
					String beatmap_id = idSet.getString("beatmap_id");
					String score = (idSet.getString("score"));
					String count300 = (idSet.getString("count300"));
					String count100 = (idSet.getString("count100"));
					String count50 = (idSet.getString("count50"));
					String countmiss = (idSet.getString("countmiss"));
					String combo = (idSet.getString("combo"));
					String perfect = (idSet.getString("perfect"));
					String enabled_mods = (idSet.getString("enabled_mods"));
					String date_played = (idSet.getString("date_played"));
					String rank = (idSet.getString("rank"));
					String pp = (idSet.getString("pp"));
					String replay_available = (idSet.getString("replay_available"));

					String c = ",";
					String q = "'";
					String query = "insert into maxscore values (" + user_id + c + beatmap_id + c + score + c + count300
							+ c + count100 + c + count50 + c + countmiss + c + combo + c + perfect + c + q
							+ enabled_mods + q + c + q + date_played + q + c + q + rank + q + c + pp + c
							+ replay_available + ") "
							+ "on conflict on constraint maxscore_pkey do update set score = excluded.score, count300 = EXCLUDED.count300, "
							+ "count100 = EXCLUDED.count100, count50 = EXCLUDED.count50, countmiss = EXCLUDED.countmiss, combo = EXCLUDED.combo, "
							+ "perfect = EXCLUDED.perfect, enabled_mods = EXCLUDED.enabled_mods, date_played = EXCLUDED.date_played, rank = EXCLUDED.rank, "
							+ "pp = EXCLUDED.pp, replay_available = EXCLUDED.replay_available where EXCLUDED.score > maxscore.score;";

					total += s2.executeUpdate(query);
					counter++;
					if (counter % 1000 == 0) {
						System.out.println(total);
					}
            }
            
                
                
            }
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	public static void updateCompletionists() {
		ArrayList<String> user_list = new ArrayList<>();
		user_list.add("6245906");
		user_list.add("1023489");
		user_list.add("2927048");
		user_list.add("3172980");
		user_list.add("7635621");
		user_list.add("4781004");
		user_list.add("2264338");
		user_list.add("47844");
		user_list.add("9217626");
		user_list.add("5444914");
		
		try {
			Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            Statement s = connection.createStatement();
            s.executeUpdate("delete from completionist");
            for(String user : user_list) {
            	s.executeUpdate("insert into completionist select beatmaps.beatmap_id, coalesce(user_id, " + user + "), "
            			+ "case when enabled_mods in (select value from mods where name like '%HT%' or name like '%EZ%' or name like '%NF%') then 1 "
            			+ "when countmiss > 0 then 2 when countmiss = 0 and (maxcombo - combo) > count100 then 3 "
            			+ "when (maxcombo - combo) <= count100 and countmiss = 0 then 4 else 0 end as status "
            			+ "from beatmaps left outer join (select * from maxscore where user_id = " + user + ") a on beatmaps.beatmap_id = a.beatmap_id where mode = 0");
            	System.out.println(user);
            }
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
	
	
}
