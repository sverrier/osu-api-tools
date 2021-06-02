package osuadvancedstats;

import org.json.simple.JSONObject;

public class Score {
	//api fields
	public String score_id;
	public String user_id;
	public String score;
	public String username;
	public String count300;
	public String count100;
	public String count50;
	public String countmiss;
	public String maxcombo;
	public String countkatu;
	public String countgeki;
	public String perfect;
	public Mods enabled_mods;
	public String date_played;
	public String rank;
	public String pp;
	public String replay_available;
	
	//non-api fields
	public String beatmap_id;
	public float ppv1;
	
	public Score(JSONObject j) {
		this.score_id = getString(j.get("score_id"));
		this.user_id = getString(j.get("user_id"));
		this.beatmap_id = getString(j.get("beatmap_id"));
		this.score = getString(j.get("score"));
		this.username = getString(j.get("username"));
		this.count300 = getString(j.get("count300"));
		this.count100 = getString(j.get("count100"));
		this.count50 = getString(j.get("count50"));
		this.countmiss = getString(j.get("countmiss"));
		this.maxcombo = getString(j.get("maxcombo"));
		this.countkatu = getString(j.get("countkatu"));
		this.countgeki = getString(j.get("countgeki"));
		this.perfect = getString(j.get("perfect"));
		this.enabled_mods = new Mods(getString(j.get("enabled_mods")));
		this.date_played = getString(j.get("date"));
		this.rank = getString(j.get("rank"));
		this.pp = getString(j.get("pp"));
		this.replay_available = getString(j.get("replay_available"));
	}
	
	public void setBeatmapId(String beatmapID) {
		this.beatmap_id = beatmapID;
	}
	
	public void setUserId(String userID) {
		this.user_id = userID;
	}
	
	public String getString(Object o) {
        if (o == null) {
            return "-1";
        } else {
            return o.toString();
        }
    }
	
	public String toCsv() {
		String s = score_id + ", " + user_id + ", " + beatmap_id + ", " + score + ", " + count300 + ", " + 
				count100 + ", " + count50 + ", " + countmiss + ", " + maxcombo + ", " + perfect + ", " + 
				enabled_mods.getValue() + ", " + date_played + ", " + rank + ", " + pp + ", " + replay_available + "\n";
		return s;
	}
	
	/*
	 * Returns an insert query formatted for my own database
	 * Primary key should be user+beatmap, simulating the displayed score
	 */
	public String getInsert(String tableName, String constraintName) {
		String c = ",";
		String q = "'";
		
		String query = "insert into " + tableName + " values (" + user_id + c + beatmap_id + c + score
				+ c + count300 + c + count100 + c + count50 + c + countmiss + c + maxcombo + c
				+ perfect + c + q + enabled_mods.getValue() + q + c + q + date_played + q + c + q + rank
				+ q + c + pp + c + replay_available + ") "
				+ "on conflict on constraint " + constraintName + " do update set score = excluded.score, count300 = EXCLUDED.count300, "
				+ "count100 = EXCLUDED.count100, count50 = EXCLUDED.count50, countmiss = EXCLUDED.countmiss, combo = EXCLUDED.combo, "
				+ "perfect = EXCLUDED.perfect, enabled_mods = EXCLUDED.enabled_mods, date_played = EXCLUDED.date_played, rank = EXCLUDED.rank, "
				+ "pp = EXCLUDED.pp, replay_available = EXCLUDED.replay_available where EXCLUDED.score > " + tableName + ".score;";
		
		return query;
	}
	
	/*
	 * Primary key here should be the scoreID
	 */
	public String getInsertWithScoreID(String tableName) {
		String c = ",";
		String q = "'";

		String query = "INSERT INTO " + tableName + " VALUES (" + score_id + c + user_id + c + beatmap_id
				+ c + score + c + count300 + c + count100 + c + count50 + c + countmiss + c
				+ maxcombo + c + perfect + c + q + enabled_mods + q + c + q + date_played + q + c
				+ q + rank + q + c + pp + c + replay_available + ") ON CONFLICT DO NOTHING;";
		
		
		return query;
	}
}
