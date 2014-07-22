package fr.sciencespo.medialab.anta;

import java.math.BigDecimal;

public class HoldMetrics {

	private int BD_SCALE = 10;
	private int BD_ROUND = BigDecimal.ROUND_HALF_EVEN;
	
	// Hi,j : hold of Wi on Wj.
	public BigDecimal hijBD;
	
	// Hj,i : hold of Wj on Wi.
	public BigDecimal hjiBD;
	
	public HoldMetrics() {
		hijBD = BigDecimal.ZERO;
		hijBD.setScale(BD_SCALE, BD_ROUND);
		hjiBD = BigDecimal.ZERO;
		hjiBD.setScale(BD_SCALE, BD_ROUND);
	}
}
