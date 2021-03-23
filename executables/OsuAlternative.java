package executables;

import java.util.Scanner;
import java.util.Timer;

import osuadvancedstats.DatabaseController;
import osuadvancedstats.NeverBeenFCedController;
import osuadvancedstats.NeverBeenSSedController;
import osuadvancedstats.OsuQuerier;
import osuadvancedstats.ProjectYearToDate;

public class OsuAlternative {
	public static void main(String[] args) {
		
        Scanner scan = new Scanner(System.in);
		
		System.out.println("API key: \n");
        String k = scan.nextLine();
        
        scan.close();
        
		DatabaseController s = new DatabaseController(k, 5000, 0);
		DatabaseController l = new DatabaseController(k, 2000, 1);
        NeverBeenSSedController s_nbss = new NeverBeenSSedController(k, 10000, 0);
        NeverBeenSSedController l_nbss = new NeverBeenSSedController(k, 10000, 0);
        NeverBeenFCedController nbfc = new NeverBeenFCedController(k, 10000);
        
        
        s.start();
        //l.start();
        nbfc.start();
        s_nbss.start();
        //l_nbss.start();
	}
}
