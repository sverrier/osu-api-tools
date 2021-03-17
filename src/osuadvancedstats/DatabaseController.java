/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package osuadvancedstats;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.TimerTask;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author samuel
 */
public class DatabaseController extends TimerTask {
    private String key = "";
    private int task;
    private int counter = 0;
    
    private Connection connection;
    private UtilAPI api;

    public DatabaseController(String k, int task, int delay) {
    	try {
	        this.key = k;
	        this.task = task;
			Class.forName("org.postgresql.Driver");
			this.connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			api = new UtilAPI(k, delay);
			
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    }
    
    @Override
    public void run() {
    	
    	switch(task) {
    	case 2:
    		topHundredLookup("2007-06-01 00:00:00", "2020-12-31 23:59:59");
    		break;
    	case 3:
    		fetchBeatmaps("2021-02-01");
    		break;
    	case 4:
    		//newFCs(delay);
    		break;
    	case 5:
    		//newSSs(100, "stars", delay);
    		break;
    	case 6:
    		break;
    	case 7:
    		updatePriorityPlayers(1000);
    		break;
    	case 9:
    		OsuQuerier.bonusQueries();
    		break;
    	default:
    		break;
    	}
    }
    
    /*
     * Fetches the ranked beatmaps starting from a given date
     */
    public void fetchBeatmaps(String since) {
        try {
            Statement s = connection.createStatement();
            int counter = 0;
            int total = 0;
            while (true) {		
            	ArrayList<Beatmap> beatmaps = api.getBeatmaps(since);
            	for(Beatmap b : beatmaps) {
            		if (b.approved.equals("1") || b.approved.equals("2")) {
            			String insert = b.getInsert("beatmaps");
            			int isNew = s.executeUpdate(insert);
	            		counter += isNew;
	                    if(isNew != 0 && b.mode.equals("0")) {
	                    	insert = "insert into neverbeenfced values(" + b.beatmap_id + ") ON CONFLICT DO NOTHING;";
	                    	s.executeUpdate(insert);
	                    	insert = "insert into neverbeenssed values(" + b.beatmap_id + ") ON CONFLICT DO NOTHING;";
	                    	s.executeUpdate(insert);
	                    }
            		}
            	}
            	
            	String newTime = beatmaps.get(beatmaps.size() - 1).approved_date;
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");  
            	since = LocalDateTime.parse(newTime, format).minusHours(1).format(format);
            	
                total += counter;
            	if (counter == 0) {
                	break;
                }
            	counter = 0;
                
                System.out.println(total);
                System.out.println(since);
            }
            s.close();

        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
        } 
    }
    
    
    
    
    /*
     * Fetches the available scores from a list of maps
     * Optionally, can have a specific user or mods specified
     * (topHundred flag is temporary - optimized table)
     */
    public void fetchScoresFromBeatmapIDs(ResultSet idSet, String user_id, String mod, boolean tophundred) {
    	try {
    		int counter = 0;
			int total = 0;
			int total2 = 0;
			
			Statement s = connection.createStatement();
			
			while (idSet.next()) {
				counter++;
				String beatmap_id = idSet.getString("beatmap_id");
				ArrayList<Score> scores = this.api.getScores(beatmap_id, user_id, mod);
				
				//insert the scores into their tables
				for(Score score : scores) {
					String insert = score.getInsertWithScoreID("scores");
					total += s.executeUpdate(insert);
					insert = score.getInsert("maxscore", "maxscore_pkey");
					total2 += s.executeUpdate(insert);
					if(tophundred) {
						insert = score.getInsert("tophundred", "tophundred_pkey");
						s.executeUpdate(insert);
					}
				}
				System.out.println("Beatmap #" + beatmap_id + "; index " + counter +"; total updates = " + total + "; maxscore updates = " + total2);
			}
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    	
    }
        
    /*
     * Fetch the available scores on each beatmap within the specified dates
     */
    public void topHundredLookup(String start, String end) {
        try {
        	
            Statement s = connection.createStatement();
            ResultSet idSet = s.executeQuery("select beatmap_id from beatmaps where mode = 0 and approved_date BETWEEN '" + start + "' and '" + end + "' order by approved_date");
            
            fetchScoresFromBeatmapIDs(idSet, null, null, true);
            
        } catch(Exception e) {
        	System.out.println(e.getLocalizedMessage());
        }
    }
    
    public void topHundredPP(int limit) {
    	try {
    		Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            Statement s = connection.createStatement();
            ResultSet idSet = s.executeQuery("select user_id from users limit " + limit);
            int total = 0;
            int total2 = 0;
            int counter = 0;
            while(idSet.next()) {
            	String user_id = idSet.getString("user_id");
            	ArrayList<Score> scores = api.getUserBest(user_id);
            	for(Score score : scores) {
            		String insert = score.getInsertWithScoreID("scores");
            		total += s.executeUpdate(insert);
            		insert = score.getInsert("maxscore", "maxscore_pkey");
            		total2 += s.executeUpdate(insert);
					counter++;		
            	} 
            	System.out.println("Users processed: " + counter + ", total new plays fetched: " + total + "; maxscore updates = " + total2);
            }
            s.close();
    	} catch (Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    }
    
    public void multiModLookup(String start, String end, String mods[]) {
    	int total = 0;
    	for(String mod : mods) {
    		topHundredLookupModded(start, end, mod);
    		System.out.println("Mod combo: " + mod + " complete, " + total + " records inserted");
    	}
    }
    
	public void topHundredLookupModded(String start, String end, String mod) {
		try {
			Statement s = connection.createStatement();
			ResultSet idSet = s.executeQuery("select beatmap_id from beatmaps where mode = 0 and approved_date BETWEEN "
					+ start + " and " + end + " order by approved_date");

			fetchScoresFromBeatmapIDs(idSet, null, mod, false);
			
			s.close();
        } catch (Exception e) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, e);
        }         
    }
    
	/*
	 * Unsure how to handle this one as there is no score ID
	 */
    public void fetchRecent(String userID) {
    	try {
		    Statement s = connection.createStatement();
		    
		    ArrayList<Score> scores = api.getUserRecent(userID);
            
            s.close();
            
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
    }
    
    public void playerLookupSpecific(int limit) {
    	try {
    		Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			Statement s = connection.createStatement();
			Statement s2 = connection.createStatement();
			ResultSet temp  = s.executeQuery("select * from queue limit " + limit);
			while (temp.next()) {
				String mindiff = temp.getObject("diff_min").toString();
				String maxdiff = temp.getObject("diff_max").toString();
				String userID = temp.getObject("user_id").toString();
				String start = temp.getObject("date_min").toString();
				String end = temp.getObject("date_max").toString();
				String optimized = temp.getObject("optimized").toString();
				
				String q = "select beatmap_id from beatmaps where mode = 0 and approved_date BETWEEN '" + start + "' and '" + end
						+ "' and stars >= " + mindiff + " and stars < " + maxdiff;
				if(optimized.equals("1")) {
					q = q + " and beatmap_id not in (select distinct beatmap_id from maxscore where user_id = " + userID + ")";
				}
				if(optimized.equals("2")) {
					q = q + " and beatmap_id not in (select distinct a.beatmap_id from (select * from maxscore where user_id = " + userID + " and countmiss = 0) a inner join beatmaps on a.beatmap_id = beatmaps.beatmap_id where count100 >= (maxcombo - combo))";
				}
				if(optimized.equals("3")) {
					q = q + " and beatmap_id not in (select distinct beatmap_id from maxscore where user_id = " + userID + " and rank like '%X%')";
				}
				q = q + " order by approved_date";
				
				System.out.println(q);
				
				ResultSet idSet = s2.executeQuery(q);
				
	            fetchScoresFromBeatmapIDs(idSet, userID, null, false);

				s2.executeUpdate("delete from queue where user_id = " + userID);
			}
			
			s.close();
			s2.close();
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    }
    
    public void playerBeatmapLookup(int delay) {
    	try {
    		Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
			Statement s = connection.createStatement();
			ResultSet temp  = s.executeQuery("select * from scorequeue");
			
			Statement s2 = connection.createStatement();
			
			int total = 0;
			while (temp.next()) {
				String userID = temp.getObject("user_id").toString();
				String beatmapID = temp.getObject("beatmap_id").toString();
				
				ArrayList<Score> scores = api.getScores(beatmapID, userID, null);
				
				for(Score score : scores) {
					String insert = score.getInsert("maxscore", "maxscore_pkey");
					total += s2.executeUpdate(insert);
					insert = score.getInsertWithScoreID("scores");
					s2.executeUpdate(insert);
				}
				System.out.println("User: " + userID + "; Beatmap #" + beatmapID + "; index " + counter + "; updates = " + total);
			}
			
			s.executeUpdate("delete from scorequeue");
			s.close();
			s2.close();
			
    	} catch(Exception e) {
    		System.out.println(e.getLocalizedMessage());
    	}
    }

	public void fetchPlayers(String tableName, int limit, int delay) {
		try {
			Statement s = connection.createStatement();
            Statement s2 = connection.createStatement();
            ResultSet idSet = s.executeQuery("select * from backupuser limit " + limit);
            int total = 0;
            
            while(idSet.next()) {
                String userID = idSet.getString("user_id");   
                User user = fetchPlayer(userID);
                if (user != null) {
                	s2.executeUpdate("delete from " + tableName + " where user_id = " + userID);
	                total += s2.executeUpdate(user.getInsert(tableName));
	                System.out.println("New players fetched: " + total + "; latest: " + userID);
                }
                s2.executeUpdate("delete from backupuser where user_id = " + userID);
            }
            
            s.close();
            s2.close();
            
        } catch(Exception ex) {
            System.out.println(ex.getLocalizedMessage());
        } 
    }
	
	public User fetchPlayer(String userID) throws InterruptedException {
		return api.getUser(userID);
	}
    
    public void updatePlayers(String tableName, int limit, int delay) {
        try {	
            Statement s = connection.createStatement();
            ResultSet idSet = s.executeQuery("select user_id from " + tableName + " limit " + limit + ";");
            int total = 0;
            
            while(idSet.next()) {
            	boolean complete = false;
            	while(!complete) {
            		try {
						String userID = idSet.getString("user_id");
						URL url = new URL("https://osu.ppy.sh/api/get_user?k=" + key + "&u=" + userID);
						Thread.sleep(delay);
						BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
						String inputLine;
						while ((inputLine = in.readLine()) != null) {
							JSONParser parser = new JSONParser();
							JSONArray json = (JSONArray) parser.parse(inputLine);
							
							if(json.size() == 0) {
								complete = true;
							}

							for (int i = 0; i < json.size(); i++) {
								JSONObject j = (JSONObject) json.get(i);
								String user_id = userID;
								String username = toString(j.get("username")).replaceAll("'", "''");
								String join_date = toString(j.get("join_date"));
								String count300 = toString(j.get("count300"));
								String count100 = toString(j.get("count100"));
								String count50 = toString(j.get("count50"));
								String playcount = toString(j.get("playcount"));
								String ranked_score = toString(j.get("ranked_score"));
								String total_score = toString(j.get("total_score"));
								String pp_rank = toString(j.get("pp_rank"));
								String pp = toString(j.get("pp_raw"));
								String level = toString(j.get("level"));
								String accuracy = toString(j.get("accuracy"));
								String ssh_count = toString(j.get("count_rank_ssh"));
								String ss_count = toString(j.get("count_rank_ss"));
								String sh_count = toString(j.get("count_rank_sh"));
								String s_count = toString(j.get("count_rank_s"));
								String a_count = toString(j.get("count_rank_a"));
								String country = toString(j.get("country"));
								String playtime = toString(j.get("total_seconds_played"));

								String c = ",";
								String q = "'";
								String insert = "INSERT INTO USERS VALUES (" + user_id + c + q + username + q + c + q
										+ join_date + q + c + count300 + c + count100 + c + count50 + c + playcount + c
										+ ranked_score + c + total_score + c + pp_rank + c + pp + c + level + c
										+ accuracy + c + ssh_count + c + ss_count + c + sh_count + c + s_count + c
										+ a_count + c + q + country + q + c + playtime
										+ ") ON CONFLICT (user_id) DO UPDATE SET username = " + q + username + q
										+ ",count300 = " + count300 + ",count100 = " + count100 + ",count50 = "
										+ count50 + ",playcount = " + playcount + ",pp = " + pp + ",ssh_count = "
										+ ssh_count + ",ss_count = " + ss_count + ",sh_count = " + sh_count
										+ ",s_count = " + s_count + ",a_count = " + a_count + ",ranked_score = " + ranked_score
										+ ",total_score = " + total_score + ",level = " + level + ",pp_rank = " + pp_rank 
										+ ",accuracy = " + accuracy + ",playtime = " + playtime + ",country = " + q + country + q + ";";

								Statement s2 = connection.createStatement();
								total += s2.executeUpdate(insert);
								System.out.println("Updated player " + username + ", total fetched: " + total);
								complete = true;
							}
						}
						
            		} catch(Exception e) {
            			System.out.println(e.getLocalizedMessage());
            			Thread.sleep(10000);
            		}
            	}
            }
            
            connection.close();
            
        } catch(Exception e) {
        	System.out.println(e.getLocalizedMessage());
        }
        
    }
    
    public void updatePriorityPlayers(int limit) {
        try {	
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            Statement s = connection.createStatement();
            Statement s2 = connection.createStatement();
            ResultSet idSet = s.executeQuery("select user_id from priorityuser limit " + limit + ";");
            int total = 0;
            
            while(idSet.next()) {
				String user_id = idSet.getString("user_id");
				User user = api.getUser(user_id);
							
				if(user != null) {
					String insert = user.getInsert("users");
					s2.executeUpdate("delete from users where user_id = " + user.user_id);
					total += s2.executeUpdate(insert);
					System.out.println("Updated player " + user.username + ", total fetched: " + total);
				}
            }		
            
            s.close();
            s2.close();
            
        } catch(Exception e) {
        	System.out.println(e.getLocalizedMessage());
        }
        
    }
    
    public void fetchMappers(int limit, int delay) {
        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/osu", "postgres", "root");
            Statement s = connection.createStatement();
            ResultSet idSet = s.executeQuery("select creator_id, count(*) from beatmaps where creator_id NOT IN (select user_id from users) group by creator_id order by count desc limit " + limit);
            int total = 0;
            
            while(idSet.next()) {
                String userID = idSet.getString("creator_id");
                URL url = new URL("https://osu.ppy.sh/api/get_user?k=" + key + "&u=" + userID );
                Thread.sleep(delay);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                url.openStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    JSONParser parser = new JSONParser();
                    JSONArray json = (JSONArray) parser.parse(inputLine);
                    
                    for (int i = 0; i < json.size(); i++) {
                        JSONObject j = (JSONObject) json.get(i);
                        String user_id = userID;
                        String username = toString(j.get("username")).replaceAll("'", "''");
                        String join_date = toString(j.get("join_date"));
                        String count300 = toString(j.get("count300"));
                        String count100 = toString(j.get("count100"));
                        String count50 = toString(j.get("count50"));
                        String playcount = toString(j.get("playcount"));
                        String ranked_score = toString(j.get("ranked_score"));
                        String total_score = toString(j.get("total_score"));
                        String pp_rank = toString(j.get("pp_rank"));
                        String pp = toString(j.get("pp_raw"));
                        String level = toString(j.get("level"));
                        String accuracy = toString(j.get("accuracy"));
                        String ssh_count = toString(j.get("count_rank_ssh"));
                        String ss_count = toString(j.get("count_rank_ss"));
                        String sh_count = toString(j.get("count_rank_sh"));
                        String s_count = toString(j.get("count_rank_s"));
                        String a_count = toString(j.get("count_rank_a"));
                        String country = toString(j.get("country"));
                        String playtime = toString(j.get("total_seconds_played"));
                        
                        String c = ",";
                        String q = "'";
                        String insert = "INSERT INTO USERS VALUES ("+user_id+c+q+username+q+c+q+join_date+q+c+count300+c+count100
                                +c+count50+c+playcount+c+ranked_score+c+total_score+c+pp_rank+c+pp+c+level+c+accuracy+c+ssh_count
                                +c+ss_count+c+sh_count+c+s_count+c+a_count+c+q+country+q+c+playtime + ") ON CONFLICT DO NOTHING;";
                        
                        Statement s2 = connection.createStatement();
                        total += s2.executeUpdate(insert);
                        System.out.println("New players fetched: " + total);
                    }
                }
            }
            
            connection.close();
            
        } catch(SQLException ex) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MalformedURLException ex) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    public String toString(Object o) {
        if (o == null) {
            return "-1";
        } else {
            return o.toString();
        }
    }
}
