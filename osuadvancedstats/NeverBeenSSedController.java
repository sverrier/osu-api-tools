package osuadvancedstats;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NeverBeenSSedController extends Thread {
	private Connection connection;
    private UtilAPI api;
    
    private int task = 0;

	public NeverBeenSSedController(String k, int delay, int task) {
		try {
			Class.forName("org.postgresql.Driver");
			this.connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			this.task = task;
			api = new UtilAPI(k, delay);
			
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}	
	}
	
	public void run() {
		if(task == 0) {
			while(true) {
				newSSs(100, "stars");
			}
		} else {
			newSSs(5000, "stars");
			uniqueSSs(5000);
		}
	}
	
	public void newSSs(int limit, String order) {
    	try {
            Statement s = connection.createStatement();
			Statement s2 = connection.createStatement();

            ResultSet idSet = s.executeQuery("select neverbeenssed.beatmap_id from neverbeenssed inner join beatmaps on neverbeenssed.beatmap_id = beatmaps.beatmap_id order by " + order + " asc limit " + limit);
            
            int scoreUpdates = 0;
			while (idSet.next()) {
				String map_id = idSet.getString("beatmap_id");
				boolean found = false;
				String mods[] = {null, "0", "8", "16416"};
				for(String mod : mods) {
					if(found) {
						break;
					}
					ArrayList<Score> scores = api.getScores(map_id, null, mod);
					for (Score score : scores) {
						if(score.rank.contains("X") && !score.enabled_mods.isHalTime() && !score.enabled_mods.isEasy()) {
							String query = "insert into newSSs values (" + score.beatmap_id + "," + score.user_id + ",'" + score.date_played + "');";
							scoreUpdates += s2.executeUpdate(query);
							String c = ",";
							String q = "'";
							
							query = "insert into unique_ss values (" + score.beatmap_id + c + score.user_id + ") on conflict do nothing";
							
							s2.executeUpdate(query);
							
							System.out.println("New SS: " + score.beatmap_id + "," + score.user_id + "," + score.date_played + ", " + scoreUpdates + " updates");
							query = "delete from neverbeenssed where beatmap_id = " + score.beatmap_id;
							s2.executeUpdate(query);
							found = true;
							break;
						}
					}
				}
			}

			s.close();
			s2.close();

        } catch (Exception e) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, e);
        } 
    }
		
	public void uniqueSSs(int limit) {
    	try {
            Statement s = connection.createStatement();	   
			Statement s2 = connection.createStatement();
            ResultSet idSet = s.executeQuery("select unique_ss.beatmap_id from unique_ss inner join beatmaps on unique_ss.beatmap_id = beatmaps.beatmap_id order by stars limit " + limit);
			while (idSet.next()) {
				String map_id = idSet.getString("beatmap_id");
				int ss_count = 0;
				ArrayList<String> userIDs = new ArrayList<>();
				ArrayList<ArrayList<Score>> scores = new ArrayList<>();
				
				//Check the nomod, hidden, global, and perfect boards
				scores.add(api.getScores(map_id, null, null));
				scores.add(api.getScores(map_id, null, "0"));
				scores.add(api.getScores(map_id, null, "8"));
				scores.add(api.getScores(map_id, null, "16416"));
				for(ArrayList<Score> list : scores) {
					for (Score score : list) {
						if(score.rank.contains("X") && !score.enabled_mods.isHalTime() && !score.enabled_mods.isEasy()) {
							if(!userIDs.contains(score.user_id)) {
								ss_count++;
								userIDs.add(score.user_id);
							}
						}
					}
					
					if(ss_count > 1) {
						System.out.println("Not unique: " + map_id);
						s2.executeUpdate("delete from unique_ss where beatmap_id = " + map_id);
						break;
						
					}
				}
			}
			
			s.close();
			s2.close();
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    }
}
