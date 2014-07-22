package fr.sciencespo.medialab.anta;

import java.math.BigDecimal;

import org.apache.lucene.search.Query;

public class TermMetrics {
	
	public Query termQuery;
	
	public long termFreqAllDocs;
	
	// Ci : termDocFreq
	// occurrence frequency of the word Wi
	// how many documents mention the word Wi
	public BigDecimal cBD;
	
}
