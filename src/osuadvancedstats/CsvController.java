package osuadvancedstats;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class CsvController {
	private String key;
	private UtilAPI api;
	
	public CsvController(String k, int delay) {
		this.key = k;
		api = new UtilAPI(k, delay);
	}
	
	public ArrayList<String> getBeatmapIDs(String start, String end) {
		ArrayList<String> ids = new ArrayList<>();
		try {
	        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");  
	        LocalDateTime last = LocalDateTime.parse(end, format);
			while (true) {	
		        int counter = 0;
				LocalDateTime current = LocalDateTime.parse(start, format);
				if(current.isAfter(last)) {
					break;
				}
				
	        	ArrayList<Beatmap> beatmaps = api.getBeatmaps(start);
	        	for(Beatmap b : beatmaps) {
	        		if (b.approved.equals("1") || b.approved.equals("2")) {
	        			ids.add(b.beatmap_id);
	        			counter++;
	        		}
	        	}
	        	
	        	if(counter == 0) {
	        		break;
	        	}
	        	counter = 0;
	        	String newTime = beatmaps.get(beatmaps.size() - 1).approved_date;      
	        	start = LocalDateTime.parse(newTime, format).minusHours(1).format(format);
	
	            System.out.println(start);
			}
		} catch(Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
		return ids;
	}
	
	public void getScoresOneUser(ArrayList<String> beatmaps, String user_id) {
        try {
        	FileWriter out = new FileWriter("./" + user_id + ".csv");
			int counter = 0;
			int total = 0;
			int total2 = 0;
			for(String beatmap_id : beatmaps) {
				counter++;
				ArrayList<Score> scores = api.getScores(beatmap_id, user_id, null);
				for(Score score : scores) {
					out.write(score.toCsv());
				}
				System.out.println("User: " + user_id + "; Beatmap #" + beatmap_id + "; index " + counter
					+ "; total updates = " + total + "; maxscore updates = " + total2);
			}
			
			out.close();
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
		}
	}
}
