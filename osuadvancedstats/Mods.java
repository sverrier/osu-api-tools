package osuadvancedstats;

public class Mods {
	public boolean[] enabledMods;
	
	public Mods(String mods) {
		this.enabledMods = new boolean[36];
		long l = Long.parseLong(mods);
        int top = (int)(Math.log(l) / Math.log(2));
        
        //Convert total value to bitwise enum
		for(int i = 0; i <= top; i++) {
			enabledMods[i] = (l % 2 == 1);
			l = l / 2;
		}
		
	}
	
	
	public boolean isHalTime() {
		return enabledMods[8];
	}
	
	public boolean isNoFail() {
		return enabledMods[0];
	}
	
	public boolean isEasy() {
		return enabledMods[1];
	}
	
	/*
	 * Returns the combined valuation of the enabled mods
	 */
	public String getValue() {
		long sum = 0;
		for(int i = 0; i < 36; i++) {
			sum += (enabledMods[i] ? 1 : 0) * Math.pow(2, i);
		}
		return String.valueOf(sum);
	}
	
	
	
}
