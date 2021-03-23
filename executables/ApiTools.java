package executables;

import java.util.ArrayList;
import java.util.Scanner;

import osuadvancedstats.*;

public class ApiTools {
	public static void main(String[] args) {
		
        Scanner scan = new Scanner(System.in);
        
        System.out.println("API key: \n");
        String k = scan.nextLine();
        
        System.out.println("Calls per minute: \n");
        int delay = 60000 / Integer.parseInt(scan.nextLine());
        
        System.out.println("Output type: csv, json\n");
        String output = scan.nextLine();
        
        if(output.equalsIgnoreCase("json")) {
        	
        } else if (output.equalsIgnoreCase("csv")) {
        	csvController(k, delay);
        }
    	                        
        scan.close();
    }
	
	public static void csvController(String k, int delay) {
        Scanner scan = new Scanner(System.in);
		CsvController csv = new CsvController(k, delay);
		System.out.println("Data to fetch: beatmaps, scores");	
		String type = scan.nextLine();
		if(type.equalsIgnoreCase("scores")) {
	        System.out.println("User_id: ");
	        String user_id = scan.nextLine();
	        
	        System.out.println("Start date yyyy-mm-dd hh:mm:ss: ");
	        String start = scan.nextLine();

	        System.out.println("End date yyyy-mm-dd hh:mm:ss: ");
	        String end = scan.nextLine();
	        
	        ArrayList<String> beatmaps = csv.getBeatmapIDs(start, end);
	        
	        csv.getScoresOneUser(beatmaps, user_id);


	        
		}
		scan.close();
	}
}
