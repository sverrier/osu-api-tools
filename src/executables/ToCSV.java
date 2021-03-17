package executables;

import java.util.ArrayList;
import java.util.Scanner;

import osuadvancedstats.*;

public class ToCSV {
	public static void main(String[] args) {
    	int modsfull[] = {72, 88, 24, 0, 8, 64, 1112, 600, 1104, 80, 16, 584, 1040, 1048, 576, 1024, 104, 1096, 1088, 40, 56, 1032, 112, 16416};
        int modsmain[] = {0, 8, 16, 64, 576, 72, 584, 24, 16416, 32};	
        int mods[] = {0, 8, 16, 64, 80};
        Scanner scan = new Scanner(System.in);
        
        System.out.println("API key: ");
        String k = scan.nextLine();
        
        System.out.println("Calls per minute: ");
        int delay = 60000 / Integer.parseInt(scan.nextLine());
    	
        CsvController csv = new CsvController(k, delay);
        NeverBeenSSed nbss = new NeverBeenSSed(k, delay);
        NeverBeenFCed nbfc = new NeverBeenFCed(k, delay);
        ProjectYearToDate pytd = new ProjectYearToDate(k, "2021", delay);
        
        System.out.println("User_id: ");
        String user_id = scan.nextLine();
        
        ArrayList<String> beatmaps = csv.getBeatmapIDs("2007-01-01 00:00:00", "2008-01-01 00:00:00");
        
        csv.getScoresOneUser(beatmaps, user_id);
        
        scan.close();
    	
    	/*Timer t = new Timer();
    	
    	if(args[0].equalsIgnoreCase("one")) {
    		t.scheduleAtFixedRate(oc_newbeatmaps, 0, 1*5*60*1000);
        	t.scheduleAtFixedRate(oc_2021_full, 1*3600*1000, 3*60*60*1000);
        	t.scheduleAtFixedRate(oc_2021, 0, 1*5*60*1000);
        	t.scheduleAtFixedRate(oc_newfcs, 600*1000, 1*60*60*1000);
        	t.scheduleAtFixedRate(oc_newSSs, 2400*1000, 2*60*60*1000);
    	}*/
    }
}
