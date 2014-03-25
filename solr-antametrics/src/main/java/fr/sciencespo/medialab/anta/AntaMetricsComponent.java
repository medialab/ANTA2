package fr.sciencespo.medialab.anta;

import java.io.IOException;
import java.lang.Math;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.sciencespo.medialab.anta.TermMetrics;

public class AntaMetricsComponent extends SearchComponent {
	// implements SolrCoreAware, SolrEventListener

	private static Logger LOGGER = LoggerFactory.getLogger(AntaMetricsComponent.class);
	private int BD_SCALE = 10;
	private int BD_ROUND = BigDecimal.ROUND_HALF_EVEN;
	volatile long numRequests;
	volatile long totalRequestsTime;
	protected String fieldAnalyze;
	protected String fieldUniqueKey;
	protected BigDecimal minTermDocFreq;
	protected BigDecimal minTermFreqAllDocs;

	@Override
	public void init(NamedList args) {
		super.init(args);
		fieldAnalyze = (String) args.get("fieldAnalyze");
		LOGGER.info("fieldAnalyze: " + fieldAnalyze);
		if (fieldAnalyze == null) {
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
					"Need to specify the fieldAnalyze inside antametrics searchComponent in the solrconfig.xml");
		}
		
		minTermDocFreq = new BigDecimal(Integer.parseInt((String)args.get("minTermDocFreq")));
		minTermDocFreq.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("minTermDocFreq: " + minTermDocFreq.toPlainString());
		
		minTermFreqAllDocs = new BigDecimal(Integer.parseInt((String)args.get("minTermFreqAllDocs")));
		minTermFreqAllDocs.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("minTermFreqAllDocs: " + minTermFreqAllDocs.toPlainString());
	}

	@Override
	public void prepare(ResponseBuilder rb) throws IOException {
		// Not necessary
	}

	@Override
	public void process(ResponseBuilder rb) throws IOException {
		LOGGER.info("### Starting process");
		
		// startTime to calculate elapsed time
		long startTime = System.currentTimeMillis();
		
		// increment the number of requests
		numRequests++;
		
		// Get the unique key field from the schema
		IndexSchema schema = rb.req.getSchema();
		SchemaField fieldUniqueKeyField = schema.getUniqueKeyField();
		fieldUniqueKey = fieldUniqueKeyField.getName();
		LOGGER.info("fieldUniqueKey: " + fieldUniqueKey);

		// Get from the URL request parameters the requested metrics
		SolrParams params = rb.req.getParams();
		String fieldAnalyzeParam = params.get("fieldAnalyze");
		if (fieldAnalyzeParam != null) {
			fieldAnalyze = fieldAnalyzeParam;
		}
		LOGGER.info("fieldAnalyze: " + fieldAnalyze);
		
		String[] metricsParams = params.getParams("metrics");
		
		if (metricsParams != null) {
			// Get the request Solr Index Searcher
			SolrIndexSearcher searcher = rb.req.getSearcher();
			
			// Get the request Index Reader as AtomicReader
			AtomicReader reader = searcher.getAtomicReader();

			// Create a NamedList that will hold this component response
			NamedList<Object> antaMetricsNL = new NamedList<Object>();
			// Add the antaMetricsNL NamedList response to the overall
			// response
			rb.rsp.add("antaMetrics", antaMetricsNL);

			for (String metrics : metricsParams) {
				//LOGGER.info("metrics: " + metrics);
				if (metrics.equalsIgnoreCase("tfidf")) {
					processTFIDF(antaMetricsNL, rb, reader, searcher);
				} else if (metrics.equalsIgnoreCase("gl")) {
					processGL(antaMetricsNL, rb, reader, searcher);
				} else {
					LOGGER.error("Unknown metrics: " + metrics);
				}
			}
		}

		// Request time
		long requestsTime = System.currentTimeMillis() - startTime;
		totalRequestsTime += requestsTime;
		LOGGER.info("### Ending process totalRequestsTime: " + requestsTime);
	}

	private void processTFIDF(NamedList<Object> antaMetricsNL,
			ResponseBuilder rb, AtomicReader reader, SolrIndexSearcher searcher) throws IOException {
		LOGGER.info("### Starting processTFIDF");
		
		long startTime = System.currentTimeMillis();
		
		// NamedList for tfidf
		NamedList<Object> tfidfNL = new NamedList<Object>();
		antaMetricsNL.add("tfidf", tfidfNL);
		
		LOGGER.info("### Starting processTFIDF global metrics");
		
		// Number of documents that have at least one term for this field
		BigDecimal docsCount = new BigDecimal(reader.getDocCount(fieldAnalyze));
		docsCount.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("docsCount: " + docsCount.toPlainString());
		tfidfNL.add("docsCount", docsCount.toPlainString());
		
		// All terms for this analyzed field
		Terms terms = reader.terms(fieldAnalyze);
		BigDecimal termsCount = new BigDecimal(terms.size());
		termsCount.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("termsCount: " + termsCount.toPlainString());
		tfidfNL.add("termsCount", termsCount.toPlainString());

		// Sum of TermsEnum.totalTermFreq() for all terms in this analyzed field
		BigDecimal termsFreqTotal = new BigDecimal(terms.getSumTotalTermFreq());
		termsFreqTotal.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("termsFreqTotal: " + termsFreqTotal.toPlainString());
		tfidfNL.add("termsFreqTotal", termsFreqTotal.toPlainString());
		
		long globalMetricsTime = System.currentTimeMillis();
		LOGGER.info("### Ending processTFIDF Global metrics: " + (globalMetricsTime - startTime));
		
		LOGGER.info("### Starting processTFIDF Documents iteration");
		
		// Querying all documents
		Query queryAllDocs = new MatchAllDocsQuery();
		DocSet docsSet = searcher.getDocSet(queryAllDocs, null);
		
		// docsSetSize maybe different from docsCount
		// if some documents doesn't contains the fieldAnalyze
		int docsSetSize = docsSet.size();
		LOGGER.info("docsSetSize: " + docsSetSize);
		
		if (docsSet == null || docsSetSize == 0) {
			LOGGER.info("Documents list is empty");
			
		} else {			
			// NamedList for documents list
			NamedList<Object> documentsNL = new NamedList<Object>();
			tfidfNL.add("documents", documentsNL);
			
			// Get a document iterator to look through all documents
			DocIterator docsIter = docsSet.iterator();

			// Reader fieldSet
			Set<String> fieldSet = new HashSet<String>();
			fieldSet.add(fieldAnalyze);
			fieldSet.add(fieldUniqueKey);

			// Reusable TermsEnum
			TermsEnum termsEnum = null;
			
			while (docsIter.hasNext()) {
				try {
					// Get document
					int docId = docsIter.nextDoc();
					Document doc = reader.document(docId, fieldSet);
					String docUniqueKey = doc.getField(fieldUniqueKey).stringValue();

					// NamedList for this document
					NamedList<Object> docNL = new NamedList<Object>();
					documentsNL.add(docUniqueKey, docNL);
					
					// Retrieve term vector for this document and field, or null if
					// term vectors were not indexed.
					Terms termsVector = reader.getTermVector(docId, fieldAnalyze);
					
					if (termsVector != null) {
						// Number of terms in this document
						BigDecimal docTermsCount = new BigDecimal(termsVector.size());
						docTermsCount.setScale(BD_SCALE, BD_ROUND);
						docNL.add("docTermsCount", docTermsCount.toPlainString());
						
						// NamedList for terms list
						NamedList<Object> termsNL = new NamedList<Object>();
						docNL.add("terms", termsNL);
						
						// Iterator that will step through all terms
						termsEnum = termsVector.iterator(termsEnum);

						// Reusable termBytesRef
						BytesRef termBytesRef;
						while ((termBytesRef = termsEnum.next()) != null) {
							// term reconstruction for querying
							Term term = new Term(fieldAnalyze, termBytesRef);
							
							// termText : term text as String
							String termText = termBytesRef.utf8ToString();
							
							// NamedList for each term
							NamedList<Object> termMetrics = new NamedList<Object>();
							termsNL.add(termText, termMetrics);

							// termFreq
							// we have only one doc for this termsEnum
							BigDecimal termFreq = new BigDecimal(termsEnum.totalTermFreq());
							termFreq.setScale(BD_SCALE, BD_ROUND);
							
							// termFreqAllDocs
							BigDecimal termFreqAllDocs = new BigDecimal(reader.totalTermFreq(term));
							termFreqAllDocs.setScale(BD_SCALE, BD_ROUND);
							termMetrics.add("termFreqAllDocs", termFreqAllDocs.toPlainString());

							// tf
							// term frequency
							// basic : tf(t,d) = f(t,d)
							// scaled frequency : tf(t,d) = log (f(t,d) + 1)
							// augmented frequency : raw frequency divided by the maximum raw 
							// frequency of any term in the document
							// tf(t,d) = 0.5 + (0.5 * f(t,d)) / max(f(w,d) : w E d)
							BigDecimal tf = termFreq;
							termFreq.setScale(BD_SCALE, BD_ROUND);
							termMetrics.add("tf", tf.toPlainString());

							// df
							// number of documents where the term t appears
							BigDecimal df = new BigDecimal(reader.docFreq(term));
							df.setScale(BD_SCALE, BD_ROUND);
							termMetrics.add("df", df.toPlainString());

							// idf
							BigDecimal docsCountDivideDf = docsCount.divide(df, BD_SCALE, BD_ROUND);
							docsCountDivideDf.setScale(BD_SCALE, BD_ROUND);
							BigDecimal idf = new BigDecimal(Math.log(docsCountDivideDf.doubleValue()));
							idf.setScale(BD_SCALE, BD_ROUND);
							termMetrics.add("idf", idf.toPlainString());

							// tfidf
							BigDecimal tfidf = tf.multiply(idf);
							tfidf.setScale(BD_SCALE, BD_ROUND);
							termMetrics.add("tfidf", tfidf.toPlainString());
						}
					}

				} catch (IOException ex) {
					LOGGER.error(null, ex);
				}
			}
		}
		long tfidfDuration = System.currentTimeMillis() - startTime;
		tfidfNL.add("requestTime", String.valueOf(tfidfDuration));
		LOGGER.info("### Ending processTFIDF totalRequestsTime: " + tfidfDuration);
	}

	private void processGL(NamedList<Object> antaMetricsNL, ResponseBuilder rb,
			AtomicReader reader, SolrIndexSearcher searcher) throws IOException {
		LOGGER.info("### Starting processGL");
		
		long startTime = System.currentTimeMillis();
		
		LOGGER.info("### Starting processGL Global metrics");
		
		// NamedList for gl
		NamedList<Object> glNL = new NamedList<Object>();
		antaMetricsNL.add("gl", glNL);
		
		// Number of documents that have at least one term for this field
		BigDecimal docsCount = new BigDecimal(reader.getDocCount(fieldAnalyze));
		docsCount.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("docsCount: " + docsCount.toPlainString());
		glNL.add("docsCount", docsCount.toPlainString());
		
		// All terms for this analyzed field
		Terms terms = reader.terms(fieldAnalyze);
		BigDecimal termsCount = new BigDecimal(terms.size());
		termsCount.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("termsCount: " + termsCount.toPlainString());
		glNL.add("termsCount", termsCount.toPlainString());

		// Sum of TermsEnum.totalTermFreq() for all terms in this analyzed field
		BigDecimal termsFreqTotal = new BigDecimal(terms.getSumTotalTermFreq());
		termsFreqTotal.setScale(BD_SCALE, BD_ROUND);
		LOGGER.info("termsFreqTotal: " + termsFreqTotal.toPlainString());
		glNL.add("termsFreqTotal", termsFreqTotal.toPlainString());
		
		long globalMetricsTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL Global metrics: " + (globalMetricsTime - startTime));

		
		LOGGER.info("### Starting processGL Terms filtering");
		
		// Minimum used for filtering
		glNL.add("minTermDocFreq", minTermDocFreq.toPlainString());
		LOGGER.info("minTermDocFreq: " + minTermDocFreq.toPlainString());
		glNL.add("minTermFreqAllDocs", minTermFreqAllDocs.toPlainString());
		LOGGER.info("minTermFreqAllDocs: " + minTermFreqAllDocs.toPlainString());
		
		// NamedList for terms
		NamedList<Object> termsNL = new NamedList<Object>();
		glNL.add("terms", termsNL);
		
		// Create an TreeMap of TermMetrics to store metrics sorted by term
		TreeMap<String, TermMetrics> termsMap = new TreeMap<String, TermMetrics>();

		// TermsEnum for every terms
		TermsEnum termsEnum = terms.iterator(null);
		
		// Populate termsMap with terms that above the minimums
		BytesRef termBytesRef;
		
		while ((termBytesRef = termsEnum.next()) != null) {
			// termText : term text as String
			String termText = termBytesRef.utf8ToString();
			
			// term reconstruction for querying
			Term term = new Term(fieldAnalyze, BytesRef.deepCopyOf(termBytesRef));
			
			// Global metrics
			BigDecimal termDocFreq = new BigDecimal(reader.docFreq(term));
			termDocFreq.setScale(BD_SCALE, BD_ROUND);
			BigDecimal termFreqAllDocs = new BigDecimal(reader.totalTermFreq(term));
			termFreqAllDocs.setScale(BD_SCALE, BD_ROUND);
			
			//StringBuffer debugSBTerm = new StringBuffer();
			//debugSBTerm.append("Term: ");
			//debugSBTerm.append(termText);
			//debugSBTerm.append("termDocFreq: ");
			//debugSBTerm.append(termDocFreq.toPlainString());
			//debugSBTerm.append("termTotalTermFreq: ");
			//debugSBTerm.append(termTotalTermFreq.toPlainString());
			
			// Verify the minimum : minTermDocFreq, minTermFreqAllDocs
			if (termDocFreq.compareTo(minTermDocFreq) < 0 || termFreqAllDocs.compareTo(minTermFreqAllDocs) < 0) {
				//debugSBTerm.append(" OUT");
			} else {
				//debugSBTerm.append(" IN");
				// Create a TermMetrics for storing the metrics of this term
				TermMetrics termMetrics = new TermMetrics();
				termMetrics.term = term;
				termMetrics.termText = termText;
				termMetrics.termQuery = new TermQuery(term);
				termMetrics.termFreqAllDocs = termFreqAllDocs;
				
				// Ci termDocFreq : occurrence frequency of the word Wi
				// how many documents mention the word Wi
				termMetrics.c = termDocFreq;
				termsMap.put(termText, termMetrics);
			}
		}
		BigDecimal termsFilteredCount = new BigDecimal(termsMap.size());
		termsFilteredCount.setScale(BD_SCALE, BD_ROUND);
		glNL.add("termsFilteredCount", minTermDocFreq.toPlainString());
		LOGGER.info("termsFilteredCount: " + termsFilteredCount);
		
		BigDecimal termsFilteredCountMinus1 = termsFilteredCount.subtract(BigDecimal.ONE);
		
		long termsFilteringTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL Terms filtering: " + (termsFilteringTime - globalMetricsTime));
		
		
		LOGGER.info("### Starting processGL Co-occurrence");
		
		// Create a TreeMap of BigDecimal to store Ci,j
		// co-occurrence of the words Wi and Wj
		TreeMap<String, BigDecimal> termsCijMap = new TreeMap<String, BigDecimal>();

		// Create a TreeMap of BigDecimal to store Hi,j
		// hold of Wi on Wj. Hi,j ≠ Hj,i
		TreeMap<String, BigDecimal> termsHijMap = new TreeMap<String, BigDecimal>();
		
		// Create a TreeMap of BigDecimal to store Hj,i
		// hold of Wj on Wi. Hi,j ≠ Hj,i
		TreeMap<String, BigDecimal> termsHjiMap = new TreeMap<String, BigDecimal>();

		for (TermMetrics termMetrics : termsMap.values()) {
			
			for (TermMetrics termOtherMetrics : termsMap.values()) {

				if (!termMetrics.termText.equals(termOtherMetrics.termText)) {
					//LOGGER.info("termOtherText: " + termOtherMetrics.termText);
					
					String[] keys = {termMetrics.termText, termOtherMetrics.termText};
					
					// The key used in termsHijMap will be directed i->j
					String keyDirected = StringUtils.join(keys, "_");
					
					// The key of the termsCijMap will be the two terms alphabetically ordered
					// and separated by _
					java.util.Arrays.sort(keys);
					String keySorted = StringUtils.join(keys, "_");
					
					BigDecimal cij;
					
					if (!termsCijMap.containsKey(keySorted)) {
						try {
							// Ci,j : co-occurrence of the words Wi and Wj
							// how many documents mention the words Wi and Wj
							// Ci,j = Cj,i
							cij = new BigDecimal(searcher.numDocs(termMetrics.termQuery, termOtherMetrics.termQuery));
							cij.setScale(BD_SCALE, BD_ROUND);
							termsCijMap.put(keySorted, cij);
							
						} catch (Throwable e) {
							cij = BigDecimal.ZERO;
							LOGGER.error(null, e);
						}
					} else {
						cij = termsCijMap.get(keySorted);
					}
					
					if (cij.compareTo(BigDecimal.ZERO) > 0) {
						// Hi,j = Cij/Cj : hold of Wi on Wj
						// the probability, for a document that mentions Wj, to mention Wi
						// Hi,j ≠ Hj,i
						BigDecimal hij = cij.divide(termOtherMetrics.c, BD_SCALE, BD_ROUND);
						termsHijMap.put(keyDirected, hij);
						
						// Hj,i = Cij/Ci
						BigDecimal hji = cij.divide(termMetrics.c, BD_SCALE, BD_ROUND);
						termsHjiMap.put(keyDirected, hji);
					}
				}
			}
		}
		LOGGER.info("termsCijMap.size(): " +  termsCijMap.size());
		LOGGER.info("termsHijMap.size(): " +  termsHijMap.size());
		LOGGER.info("termsHjiMap.size(): " +  termsHjiMap.size());
		long cooccurrenceTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL Co-occurrence: " + (cooccurrenceTime - termsFilteringTime));
		
		LOGGER.info("### Starting processGL SVGL Metrics");
		BigDecimal sumG = new BigDecimal(0);
		sumG.setScale(BD_SCALE, BD_ROUND);
		
		BigDecimal sumL = new BigDecimal(0);
		sumL.setScale(BD_SCALE, BD_ROUND);
		
		for (TermMetrics termMetrics : termsMap.values()) {
			
			// Si = Sum Hi,j where(i≠j) / ￼(n−1) : mean hold strength of Wi
			// the probability, for a (randomly chosen) word, to be mentioned in a
			// document that mentions Wi
			BigDecimal sumHij = calculateSum(termMetrics.termText, termsHijMap);
			termMetrics.s = sumHij.divide(termsFilteredCountMinus1, BD_SCALE, BD_ROUND);

			// Vi = Sum Hj,i where(j≠i) / ￼(n−1) : mean hold vulnerability of Wi
			// the probability, for Wi, to be mentioned in a document that mentions
			// another (randomly chosen) word
			BigDecimal sumHji = calculateSum(termMetrics.termText, termsHjiMap);
			termMetrics.v = sumHji.divide(termsFilteredCountMinus1, BD_SCALE, BD_ROUND);

			// Gi = Sum Hi,j - Hj,i where(j≠i) / ￼(n−1) = Si - Vi : genericity of Wi
			// the energy consumed by Wi
			// positive if Wi is generic, negative if Wi is specific
			termMetrics.g = termMetrics.s.subtract(termMetrics.v);
			sumG = sumG.add(termMetrics.g);

			// Li = Sum Hi,j + Hj,i where(j≠i) / ￼(n−1) = Si + Vi : locality of Wi
			// the coupling of Wi with other words : the energy exchanged by Wi
			termMetrics.l = termMetrics.s.add(termMetrics.v);
			sumL = sumL.add(termMetrics.l);
			
			NamedList<Object> termMetricsNL = new NamedList<Object>();
			termMetricsNL.add("termFreqAllDocs", termMetrics.termFreqAllDocs.toPlainString());
			termMetricsNL.add("C", termMetrics.c.toPlainString());
			termMetricsNL.add("S", termMetrics.s.toPlainString());
			termMetricsNL.add("V", termMetrics.v.toPlainString());
			termMetricsNL.add("G", termMetrics.g.toPlainString());
			termMetricsNL.add("L", termMetrics.l.toPlainString());
			
			termsNL.add(termMetrics.termText, termMetricsNL);				
		}
		// sumG
		glNL.add("sumG", sumG.toPlainString());
		LOGGER.info("sumG: " + sumG.toPlainString());
		
		// sumLdivide2
		BigDecimal two = new BigDecimal(2);
		two.setScale(BD_SCALE, BD_ROUND);
		BigDecimal sumLdivide2 = sumL.divide(two, BD_SCALE, BD_ROUND);
		glNL.add("sumLdivide2", sumLdivide2.toPlainString());
		LOGGER.info("sumLdivide2: " + sumLdivide2.toPlainString());
		
		long svglTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL SVGL Metrics: " + (svglTime - cooccurrenceTime));
		
		long glDuration = System.currentTimeMillis() - startTime;
		glNL.add("requestTime", String.valueOf(glDuration));
		LOGGER.info("### Ending processGL totalRequestsTime: " + glDuration);
	}

	
	private BigDecimal calculateSum(String termText, TreeMap<String, BigDecimal> termsHijMap) {
		StringBuffer prefixSB = new StringBuffer();
		prefixSB.append(termText);
		prefixSB.append("_");
		String fromKey = prefixSB.toString();
		prefixSB.append(Character.MAX_VALUE);
		String toKey = prefixSB.toString();
		
		NavigableMap<String, BigDecimal> subMapHij = termsHijMap.subMap(fromKey, true, toKey, true);
		BigDecimal sumHij = new BigDecimal(0);
		sumHij.setScale(BD_SCALE, BD_ROUND);
		for (BigDecimal hij : subMapHij.values()) {
			sumHij = sumHij.add(hij);
		}
		//LOGGER.info("sumHij for termText: " + termText + " " + sumHij.toPlainString());
		return sumHij;
	}

	// //////////////////////////////////
	// NamedListInitializedPlugin methods
	// //////////////////////////////////

	@Override
	public String getDescription() {
		return "AntaSolrMetricsComponent";
	}

	@Override
	public String getSource() {
		return "$URL: http://tools.medialab.sciences-po.fr/#anta";
	}

	@Override
	public String getVersion() {
		return "1.0";
	}

	@Override
	public NamedList<Object> getStatistics() {
		// Statistics panel in Solr
		NamedList<Object> all = new SimpleOrderedMap<Object>();
		all.add("requests", "" + numRequests);
		all.add("totalRequestTime(ms)", "" + totalRequestsTime);
		return all;
	}
}
