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
import java.util.Scanner;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

import osuadvancedstats.NeverBeenFCedController;
import osuadvancedstats.NeverBeenSSedController;
import osuadvancedstats.ProjectYearToDate;
import osuadvancedstats.DatabaseController;
import osuadvancedstats.OsuQuerier;

/**
 *
 * @author samue
 */
public class ProjectYTD {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
    	
        Scanner scan = new Scanner(System.in);
    	
    	System.out.println("API key: \n");
        String k = scan.nextLine();
        
        System.out.println("Calls per minute: \n");
        int delay = 60000 / Integer.parseInt(scan.nextLine());
    	
        ProjectYearToDate pytd = new ProjectYearToDate(k, "2021", delay);
        
        while(true) {
        	pytd.updateAllWeeks();
        }
        
    }

}
