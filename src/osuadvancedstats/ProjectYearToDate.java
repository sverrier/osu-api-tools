package osuadvancedstats;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ProjectYearToDate {
	
	private DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"); 
	private ArrayList<LocalDateTime> weeklyStartTimes = new ArrayList<>();
	private int delay;
	
	private Connection connection;
	private UtilAPI api;
	
	public ProjectYearToDate(String k, String year, int delay) {
		try {
			Class.forName("org.postgresql.Driver");
			this.connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			api = new UtilAPI(k, delay);
			String s = year + "-01-01 00:00:00";
	    	LocalDateTime start = LocalDateTime.parse(s, format);
	    	this.delay = delay;
			for(int i = 0; i <= 52; i++) {
	        	weeklyStartTimes.add(start.plusDays(i * 7));
	        }
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}	
		
	}
	
	public void updateOneWeek(int week) {
		String start = weeklyStartTimes.get(week - 1).format(format);
		String end = weeklyStartTimes.get(week).format(format);
		topHundredLookup(start, end, delay);
		OsuQuerier.weeklyQueries(start, end, week);
	}
	
	public void updateAllWeeks() {
		for(int i = 1; i <= 52; i++) {
			updateOneWeek(i);
		}
		OsuQuerier.yearToDateQueries();
	}
	
	public void topHundredLookup(String start, String end, int delay) {
        try {
            Statement s = connection.createStatement();
			Statement s2 = connection.createStatement();
            ResultSet idSet = s.executeQuery("select beatmap_id from beatmaps where mode = 0 and approved_date BETWEEN '" + start + "' and '" + end + "' order by approved_date");
            
			int counter = 0;
			int total = 0;
			while (idSet.next()) {
				String map_id = idSet.getString("beatmap_id");
				ArrayList<Score> scores = api.getScores(map_id, null, null);
				for(Score score : scores) {
					String query = score.getInsert("osualternative", "osualternative_pkey");
					total += s2.executeUpdate(query);
				}
				System.out.println("Beatmap #" + map_id + "; index " + ++counter +"; total updates = " + total);
            }
			s.close();
            s2.close();

        } catch (Exception e) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, e);
        } 
	}
	
	public void playerLookupOptimized(String start, String end, String user_id, int delay) {
        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			Statement s = connection.createStatement();
			Statement s2 = connection.createStatement();
			ResultSet idSet = s.executeQuery(
					"select beatmap_id from beatmaps where mode = 0 and approved_date BETWEEN '" + start + "' and '" + end
							+ "' and beatmap_id not in (select distinct beatmap_id from osualternative where user_id = " + user_id
							+ ") order by approved_date");

			int counter = 0;
			int total = 0;
			while (idSet.next()) {
				String beatmap_id = idSet.getString("beatmap_id");
				ArrayList<Score> scores = api.getScores(beatmap_id, user_id, null);
				for(Score score : scores) {
					String query = score.getInsert("osualternative", "osualternative_pkey");
					total += s2.executeUpdate(query);
				}
				System.out.println("User: " + user_id + "; Beatmap #" + beatmap_id + "; index " + ++counter + "; total updates = " + total);
			}
			
			s.close();
			s2.close();
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
}
