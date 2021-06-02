package osuadvancedstats;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NeverBeenFCedController extends Thread {
    private Connection connection;
    private UtilAPI api;

	public NeverBeenFCedController(String k, int delay) {
		try {
			Class.forName("org.postgresql.Driver");
			this.connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			api = new UtilAPI(k, delay);
			
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
		
	}
	
	public void run() {
		while(true) {
			newFCs();
			uniqueFCs();
		}
	}
	
	/*
	 * Perform a check on each beatmap without an FC
	 * Updates the list if an FC is found
	 */
	public void newFCs() {
    	try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            Statement s = connection.createStatement();
			Statement s2 = connection.createStatement();
			Statement s3 = connection.createStatement();
            ArrayList<String> mods = new ArrayList<String>();
            ResultSet idSet = s.executeQuery("select beatmaps.beatmap_id from neverbeenfced inner join beatmaps on beatmaps.beatmap_id = neverbeenfced.beatmap_id order by stars");
            
			while (idSet.next()) {
				String map_id = idSet.getString("beatmap_id");
				ResultSet beatmap = s2.executeQuery("select maxcombo from beatmaps where beatmap_id = " + map_id);
				beatmap.next();
				String maxcombo = beatmap.getString("maxcombo");
				
				ArrayList<Score> scores = api.getScores(map_id, null, null);
				for(Score score : scores) {
					if(Integer.parseInt(score.countmiss) == 0 && (Integer.parseInt(maxcombo) - Integer.parseInt(score.maxcombo)) <= Integer.parseInt(score.count100)) {
						if(!(score.enabled_mods.isHalTime() || score.enabled_mods.isEasy())) {
							String query = "insert into newfcs values (" + score.beatmap_id + "," + score.user_id + ",'" + score.date_played + "');";
							s3.executeUpdate(query);
							query = "insert into unique_fc values (" + score.beatmap_id + "," + score.user_id + ") on conflict do nothing";
							s3.executeUpdate(query);
							query = "delete from neverbeenfced where beatmap_id = " + score.beatmap_id;
							s3.executeUpdate(query);
							System.out.println(score.beatmap_id + "," + score.user_id + "," + score.date_played);
							break;
						}
					}

				}
            }
			s.close();
			s2.close();
			s3.close();
        } catch (Exception e) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, e);
        } 
    }
	
	public void uniqueFCs() {
    	try {
            Statement s = connection.createStatement();	   
			Statement s2 = connection.createStatement();
			Statement s3 = connection.createStatement();
            ResultSet idSet = s.executeQuery("select unique_fc.beatmap_id from unique_fc inner join beatmaps on unique_fc.beatmap_id = beatmaps.beatmap_id order by stars");
			while (idSet.next()) {
				String map_id = idSet.getString("beatmap_id");
				ResultSet beatmap = s2.executeQuery("select maxcombo from beatmaps where beatmap_id = " + map_id);
				beatmap.next();
				String maxcombo = beatmap.getString("maxcombo");
				int fc_count = 0;
				ArrayList<String> userIDs = new ArrayList<>();
				
				ArrayList<Score> scores = api.getScores(map_id, null, null);
				for (Score score : scores) {
					if(Integer.parseInt(score.countmiss) == 0 && (Integer.parseInt(maxcombo) - Integer.parseInt(score.maxcombo)) <= Integer.parseInt(score.count100)) {
						if(!(score.enabled_mods.isHalTime() || score.enabled_mods.isEasy())) {
							if(!userIDs.contains(score.user_id)) {
								fc_count++;
								userIDs.add(score.user_id);
							}
						}
					}
				}
				
				if(fc_count > 1) {
					System.out.println("Not unique: " + map_id);
					s3.executeUpdate("delete from unique_fc where beatmap_id = " + map_id);					
				}
			}
			
			s.close();
			s2.close();
			s3.close();
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    }
}
