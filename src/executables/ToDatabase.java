/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package executables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import osuadvancedstats.NeverBeenFCed;
import osuadvancedstats.NeverBeenSSed;
import osuadvancedstats.ProjectYearToDate;
import osuadvancedstats.DatabaseController;
import osuadvancedstats.OsuQuerier;

/**
 *
 * @author samue
 */
public class ToDatabase {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        DatabaseController oc = new DatabaseController("0a0d80c8b0472dbc05770263ced51dad0faa8686", -1, 1000);
        NeverBeenSSed nbss = new NeverBeenSSed("0a0d80c8b0472dbc05770263ced51dad0faa8686", 1000);
        NeverBeenFCed nbfc = new NeverBeenFCed("0a0d80c8b0472dbc05770263ced51dad0faa8686", 1000);
        ProjectYearToDate pytd = new ProjectYearToDate("0a0d80c8b0472dbc05770263ced51dad0faa8686", "2021", 500);
        
        DatabaseController oc_2021_full = new DatabaseController("0a0d80c8b0472dbc05770263ced51dad0faa8686", 0, 1000);
    	DatabaseController oc_2021 = new DatabaseController("0a0d80c8b0472dbc05770263ced51dad0faa8686", 1, 1000);
    	DatabaseController oc_newbeatmaps = new DatabaseController("0a0d80c8b0472dbc05770263ced51dad0faa8686", 3, 1000);
    	DatabaseController oc_newfcs = new DatabaseController("0a0d80c8b0472dbc05770263ced51dad0faa8686", 4, 1000);
    	DatabaseController oc_newSSs = new DatabaseController("0a0d80c8b0472dbc05770263ced51dad0faa8686", 5, 1000);

                
        int modsfull[] = {72, 88, 24, 0, 8, 64, 1112, 600, 1104, 80, 16, 584, 1040, 1048, 576, 1024, 104, 1096, 1088, 40, 56, 1032, 112, 16416};
        int modsmain[] = {0, 8, 16, 64, 576, 72, 584, 24, 16416, 32};
        int mods[] = {0, 8, 16, 64, 80};
        
        oc.fetchBeatmaps("2021-03-16 00:00:00");
        oc.updatePriorityPlayers(1000);
        
        pytd.updateAllWeeks();
        
        nbfc.newFCs();
        
        nbss.newSSs(5000, "start");
        nbss.uniqueSSs(5000);
    	
    	Timer t = new Timer();
    	
    	if(args.length == 0) {
    		System.exit(0);
    	}
    	
    	if(args[0].equalsIgnoreCase("zero")) {
    		while(true) {
    			//oc.topHundredLookup("2007-01-01 00:00:00", "2021-12-31 23:59:59");
    			//osuQuerier.yearlyQueries(2007, 2021);
    			oc.topHundredLookupModded("2012-01-01 00:00:00", "2021-12-31 23:59:59", "0");
    			OsuQuerier.yearlyQueries(2007, 2021);
        		OsuQuerier.bonusQueries();
    		}
    	}
    	else if(args[0].equalsIgnoreCase("one")) {
    		t.scheduleAtFixedRate(oc_newbeatmaps, 0, 1*5*60*1000);
        	t.scheduleAtFixedRate(oc_2021_full, 1*3600*1000, 3*60*60*1000);
        	t.scheduleAtFixedRate(oc_2021, 0, 1*5*60*1000);
        	t.scheduleAtFixedRate(oc_newfcs, 600*1000, 1*60*60*1000);
        	t.scheduleAtFixedRate(oc_newSSs, 2400*1000, 2*60*60*1000);
    	}
    	else if(args[0].equalsIgnoreCase("three")) {
    		nbss.newSSs(5000, "stars");
    	}
    	else if(args[0].equalsIgnoreCase("four")) {
    		OsuQuerier.yearlyQueries(2007, 2021);
    		OsuQuerier.bonusQueries();
    	}
    	else if(args[0].equalsIgnoreCase("five")) {
    		nbfc.newFCs();
    	}
    	//t.scheduleAtFixedRate(oc_bonus, 4*3600*1000, 24*3600*1000);
    	//t.scheduleAtFixedRate(oc_historical, 0*6*1000, 24*3600*1000);
    	//t.scheduleAtFixedRate(oc_tophundred, 0, 7*24*60*60*1000);
    	
    		

        //oc.updatePlayers();*/
        
        
    }

}
