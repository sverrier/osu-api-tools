package osuadvancedstats;

import org.json.simple.JSONObject;

public class Beatmap {
	public String beatmap_id;
	public String approved;
	public String submit_date;
	public String approved_date;
	public String last_update;
	public String artist;
	public String beatmapset_id;
	public String bpm;
	public String creator;
	public String creator_id;
	public String difficultyrating;
	public String diff_aim;
	public String diff_speed;
	public String diff_size;
	public String diff_overall;
	public String diff_approach;
	public String diff_drain;
	public String hit_length;
	public String source; 
	public String genre_id;
	public String language_id;
	public String title;
	public String total_length;
	public String version;
	public String file_md5;
	public String mode;
	public String tags;
	public String favorite_count;
	public String rating;
	public String playcount;
	public String passcount;
	public String count_normal;
	public String count_slider;
	public String count_spinner;
	public String max_combo;
	public String storyboard;
	public String video;
	public String download_unavailable;
	public String audio_unavailable;
	
	public Beatmap(JSONObject j) {
		this.approved = getString(j.get("approved"));
		this.submit_date = getString(j.get("submit_date"));
		this.approved_date = getString(j.get("approved_date"));
		this.last_update = getString(j.get("last_update"));
		this.artist = getString(j.get("artist"));
		this.beatmap_id = getString(j.get("beatmap_id"));
		this.beatmapset_id = getString(j.get("beatmapset_id"));
		this.bpm = getString(j.get("bpm"));
		this.creator = getString(j.get("creator"));
		this.creator_id = getString(j.get("creator_id"));
		this.difficultyrating = getString(j.get("difficultyrating"));
		this.diff_aim = getString(j.get("diff_aim"));
		this.diff_speed = getString(j.get("diff_speed"));
		this.diff_size = getString(j.get("diff_size"));
		this.diff_overall = getString(j.get("diff_overall"));
		this.diff_approach = getString(j.get("diff_approach"));
		this.diff_drain = getString(j.get("diff_drain"));
		this.hit_length = getString(j.get("hit_length"));
		this.source = getString(j.get("source"));
		this.genre_id = getString(j.get("genre_id"));
		this.language_id = getString(j.get("language_id"));
		this.title = getString(j.get("title"));
		this.total_length = getString(j.get("total_length"));
		this.version = getString(j.get("version"));
		this.file_md5 = getString(j.get("file_md5"));
		this.mode = getString(j.get("mode"));
		this.tags = getString(j.get("tags"));
		this.favorite_count = getString(j.get("favorite_count"));
		this.rating = getString(j.get("rating"));
		this.playcount = getString(j.get("playcount"));
		this.passcount = getString(j.get("passcount"));
		this.count_normal = getString(j.get("count_normal"));
		this.count_slider = getString(j.get("count_slider"));
		this.count_spinner = getString(j.get("count_spinner"));
		this.max_combo = getString(j.get("max_combo"));
		this.storyboard = getString(j.get("storyboard"));
		this.video = getString(j.get("video"));
		this.download_unavailable = getString(j.get("download_unavailable"));
		this.audio_unavailable = getString(j.get("audio_unavailable"));

	}
	
	public String getInsert(String tableName) {
		String c = ",";
		String q = "'";
		String insert = "INSERT INTO BEATMAPS VALUES ("
                + beatmap_id + c + approved + c + q + submit_date + q + c + q + approved_date + q + c + q + last_update + q + c + q + artist.replaceAll("'", "''") + q 
                + c  + beatmapset_id + c + bpm + c + q + creator.replaceAll("'", "''") + q + c + creator_id + c + difficultyrating + c + diff_aim
                + c + diff_speed + c + diff_size + c + diff_overall + c + diff_approach + c + diff_drain + c
                + hit_length + c + q + source.replaceAll("'", "''") + q + c + genre_id + c + language_id + c + q + title.replaceAll("'", "''") + q + c + total_length
                + c + q + version.replaceAll("'", "''") + q + c + q + file_md5 + q + c + mode + c + q + tags.replaceAll("'", "''") + q + c + favorite_count + c + rating + c + playcount + c
                + passcount + c + count_normal + c + count_slider + c + count_spinner + c + max_combo + c + storyboard + c + video
                + c + download_unavailable + c + audio_unavailable
                + ") ON CONFLICT DO NOTHING";
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
