package osuadvancedstats;

import org.json.simple.JSONObject;

public class User {
	public String user_id;
	public String username;
	public String join_date;
	public String count300;
	public String count100;
	public String count50;
	public String playcount;
	public String ranked_score;
	public String total_score;
	public String pp_rank;
	public String level;
	public String pp_raw;
	public String accuracy;
	public String count_rank_ss;
	public String count_rank_ssh;
	public String count_rank_s;
	public String count_rank_sh;
	public String count_rank_a;
	public String country;
	public String total_seconds_played;
	public String pp_country_rank;
	
	public User(JSONObject j) {
		this.user_id = getString(j.get("user_id"));
		this.username = getString(j.get("username"));
		this.join_date = getString(j.get("join_date"));
		this.count300 = getString(j.get("count300"));
		this.count100 = getString(j.get("count100"));
		this.count50 = getString(j.get("count50"));
		this.playcount = getString(j.get("playcount"));
		this.ranked_score = getString(j.get("ranked_score"));
		this.total_score = getString(j.get("total_score"));
		this.pp_rank = getString(j.get("pp_rank"));
		this.level = getString(j.get("level"));
		this.pp_raw = getString(j.get("pp_raw"));
		this.accuracy = getString(j.get("accuracy"));
		this.count_rank_ss = getString(j.get("count_rank_ss"));
		this.count_rank_ssh = getString(j.get("count_rank_ssh"));
		this.count_rank_s = getString(j.get("count_rank_s"));
		this.count_rank_sh = getString(j.get("count_rank_sh"));
		this.count_rank_a = getString(j.get("count_rank_a"));
		this.country = getString(j.get("country"));
		this.total_seconds_played = getString(j.get("total_seconds_played"));
		this.pp_country_rank = getString(j.get("pp_country_rank"));	
	}
	
	
	public String getInsert(String tableName) {
		String c = ",";
		String q = "'";
		String insert = "INSERT INTO " + tableName + " VALUES (" + user_id + c + q + username.replaceAll("'", "''") + q + c + q
				+ join_date + q + c + count300 + c + count100 + c + count50 + c + playcount + c
				+ ranked_score + c + total_score + c + pp_rank + c + level + c + pp_raw + c
				+ accuracy + c + count_rank_ss + c + count_rank_ssh + c + count_rank_s + c + count_rank_sh + c
				+ count_rank_a + c + q + country + q + c + total_seconds_played + c + pp_country_rank
				+ ")";
		
		return insert;
	}
	
	public String getString(Object o) {
        if (o == null) {
            return "-1";
        } else {
            return o.toString();
        }
    }
}
