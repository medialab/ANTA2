package fr.sciencespo.medialab.anta;

import java.math.BigDecimal;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

public class TermMetrics {
	
	public Term term;
	
	public String termText;
	
	public Query termQuery;
	
	public BigDecimal termFreqAllDocs;
	
	// Ci : termDocFreq
	// occurrence frequency of the word Wi : how many documents mention the word Wi
	public BigDecimal c;
	
	// Si = Sum Hi,j where(i≠j) / ￼(n−1)
	// mean hold strength of Wi : the probability, for a (randomly chosen) word, to be mentioned in a document that mentions Wi
	public BigDecimal s;
	
	// Vi = Sum Hj,i where(j≠i) / ￼(n−1)
	// mean hold vulnerability of Wi : the probability, for Wi, to be mentioned in a document that mentions another (randomly chosen) word
	public BigDecimal v;
	
	// Gi = Sum Hi,j - Hj,i where(j≠i) / ￼(n−1) = Si - Vi
	// genericity of Wi : the energy consumed by Wi : positive if Wi is generic, negative if Wi is specific
	public BigDecimal g;
	
	// Li = Sum Hi,j + Hj,i where(j≠i) / ￼(n−1) = Si + Vi
	// locality of Wi : the coupling of Wi with other words : the energy exchanged by Wi
	public BigDecimal l;
	
}
