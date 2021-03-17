package osuadvancedstats;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class UtilAPI {
	private String apiKey;
	private int delay;
	
	private int backoff = 5;
	private int magnitude = 2;
		
	public UtilAPI(String key, int delay) {
		this.apiKey = key;
		this.delay = delay;
	}
	
	public void setDelay(int delay) {
		this.delay = delay;
	}
	
	public ArrayList<Score> getScores(String beatmap_id, String user_id, String mod) throws InterruptedException {
		boolean complete = false;
		ArrayList<Score> scores = new ArrayList<>();
		while (!complete) {
			try {
				String path = "https://osu.ppy.sh/api/get_scores?k=" + apiKey + "&b=" + beatmap_id;
				if(user_id != null) {
					path = path + "&u=" + user_id;
				}
				if(mod != null) {
					path = path + "&mods=" + mod;
				}
				path = path + "&limit=100";
				
				URL url = new URL(path);
				Thread.sleep(delay);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					JSONParser parser = new JSONParser();
					JSONArray json = (JSONArray) parser.parse(inputLine);

					for (int i = 0; i < json.size(); i++) {
						JSONObject j = (JSONObject) json.get(i);
						Score score = new Score(j);
						score.setBeatmapId(beatmap_id);
						
						scores.add(score);

					}
				}
			} catch (Exception e) {
				System.out.println(e.getLocalizedMessage());
				long delay = backoff * magnitude * 1000;
				Thread.sleep(delay);
				magnitude += 1;
			}
			complete = true;
		}
		return scores;
	}
	
	public ArrayList<Score> getUserBest(String user_id) throws InterruptedException {
		boolean complete = false;
		ArrayList<Score> scores = new ArrayList<>();
		while (!complete) {
			try {
				String path = "https://osu.ppy.sh/api/get_user_best?k=" + apiKey + "&u=" + user_id + "&limit=100";
				URL url = new URL(path);
				Thread.sleep(delay);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					JSONParser parser = new JSONParser();
					JSONArray json = (JSONArray) parser.parse(inputLine);

					for (int i = 0; i < json.size(); i++) {
						JSONObject j = (JSONObject) json.get(i);
						Score score = new Score(j);
						score.setUserId(user_id);
						
						scores.add(score);

					}
				}
			} catch (Exception e) {
				System.out.println(e.getLocalizedMessage());
				long delay = backoff * magnitude * 1000;
				Thread.sleep(delay);
				magnitude += 1;
			}
			complete = true;
		}
		return scores;
	}
	
	public ArrayList<Score> getUserRecent(String user_id) throws InterruptedException {
		boolean complete = false;
		ArrayList<Score> scores = new ArrayList<>();
		while (!complete) {
			try {
				String path = "https://osu.ppy.sh/api/get_user_recent?k=" + apiKey + "&u=" + user_id + "&limit=100";
				URL url = new URL(path);
				Thread.sleep(delay);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					JSONParser parser = new JSONParser();
					JSONArray json = (JSONArray) parser.parse(inputLine);

					for (int i = 0; i < json.size(); i++) {
						JSONObject j = (JSONObject) json.get(i);
						Score score = new Score(j);
						score.setUserId(user_id);
						
						scores.add(score);

					}
				}
			} catch (Exception e) {
				System.out.println(e.getLocalizedMessage());
				long delay = backoff * magnitude * 1000;
				Thread.sleep(delay);
				magnitude += 1;
			}
			complete = true;
		}
		return scores;
	}
	
	public User getUser(String user_id) throws InterruptedException {
		boolean complete = false;
		User user = null;
		while (!complete) {
			try {
				String path = "https://osu.ppy.sh/api/get_user?k=" + apiKey + "&u=" + user_id;
				URL url = new URL(path);
				Thread.sleep(delay);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					JSONParser parser = new JSONParser();
					JSONArray json = (JSONArray) parser.parse(inputLine);
					if(json.size() == 0) {
						return null;
					}
					JSONObject j = (JSONObject) json.get(0);
					user = new User(j);
				}
			} catch (Exception e) {
				System.out.println(e.getLocalizedMessage());
				long delay = backoff * magnitude * 1000;
				Thread.sleep(delay);
				magnitude += 1;
			}
			complete = true;
		}
		return user;
	}
	
	public ArrayList<Beatmap> getBeatmaps(String since) throws InterruptedException {
		boolean complete = false;
		ArrayList<Beatmap> beatmaps = new ArrayList<>();
		while (!complete) {
			try {
				String path = "https://osu.ppy.sh/api/get_beatmaps?k=" + apiKey + "&since=" + since;
				URL url = new URL(path);
				Thread.sleep(delay);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					JSONParser parser = new JSONParser();
					JSONArray json = (JSONArray) parser.parse(inputLine);

					for (int i = 0; i < json.size(); i++) {
						JSONObject j = (JSONObject) json.get(i);
						Beatmap beatmap = new Beatmap(j);						
						beatmaps.add(beatmap);

					}
				}
			} catch (Exception e) {
				System.out.println(e.getLocalizedMessage());
				long delay = backoff * magnitude * 1000;
				Thread.sleep(delay);
				magnitude += 1;
			}
			complete = true;
		}
		return beatmaps;
	}
	
}
