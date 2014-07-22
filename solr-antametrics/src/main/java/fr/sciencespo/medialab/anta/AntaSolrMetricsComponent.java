package fr.sciencespo.medialab.anta;

import java.io.IOException;
import java.lang.Math;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AntaSolrMetricsComponent extends SearchComponent {
	// implements SolrCoreAware, SolrEventListener

	private static Logger LOGGER = LoggerFactory.getLogger(AntaSolrMetricsComponent.class);
	private int BD_SCALE = 10;
	private int BD_ROUND = BigDecimal.ROUND_HALF_EVEN;
	private String SEPARATOR = "¤";
	private int MAX_RESULTS = 1000;
	volatile long numRequests;
	volatile long totalRequestsTime;
	private String fieldAnalyze;
	private String fieldUniqueKey;
	private int minTermDocFreq;
	private int minTermFreqAllDocs;
	private String mongodbDatabase;
	private String mongodbHost;
	private int mongodbPort;
	private MongoClient mongoClient;
	private DB mongoDB;

	@Override
	public void init(NamedList args) {
		super.init(args);
		
		// Get arguments from solrconfig.xml
		fieldAnalyze = (String) args.get("fieldAnalyze");
		LOGGER.info("fieldAnalyze: " + fieldAnalyze);
		if (fieldAnalyze == null) {
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
					"Need to specify the fieldAnalyze inside antametrics searchComponent in the solrconfig.xml");
		}
		
		minTermDocFreq = ((Integer)args.get("minTermDocFreq")).intValue();
		LOGGER.info("minTermDocFreq: " + minTermDocFreq);
		
		minTermFreqAllDocs = ((Integer)args.get("minTermFreqAllDocs")).intValue();
		LOGGER.info("minTermFreqAllDocs: " + minTermFreqAllDocs);
		
		mongodbHost = (String)args.get("mongodbHost");
		LOGGER.info("mongodbHost: " + mongodbHost);
		
		mongodbPort = ((Integer)args.get("mongodbPort")).intValue();
		LOGGER.info("mongodbPort: " + mongodbPort);
		
		mongodbDatabase = (String)args.get("mongodbDatabase");
		LOGGER.info("mongodbDatabase: " + mongodbDatabase);
		
		if (mongodbDatabase == null || mongodbHost == null || mongodbPort == 0) {
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
					"Need to specify the mongodb properties (Host, Port, Database) inside antametrics searchComponent in the solrconfig.xml");
		}
	}

	@Override
	public void prepare(ResponseBuilder rb) throws IOException {
		// Not necessary
	}

	@Override
	public void process(ResponseBuilder rb) throws IOException {
		LOGGER.info("### Starting process");
		long startTime = System.currentTimeMillis();
		
		// increment the number of requests
		numRequests++;
		
		// Get the unique key field from the Solr schema (schema.xml)
		IndexSchema schema = rb.req.getSchema();
		SchemaField fieldUniqueKeyField = schema.getUniqueKeyField();
		fieldUniqueKey = fieldUniqueKeyField.getName();
		LOGGER.info("fieldUniqueKey: " + fieldUniqueKey);

		// Get the request SolrParams
		SolrParams params = rb.req.getParams();
		
		// Get the URL parameters
		String fieldAnalyzeParam = params.get("fieldAnalyze");
		if (fieldAnalyzeParam != null) {
			fieldAnalyze = fieldAnalyzeParam;
		}
		LOGGER.info("fieldAnalyze: " + fieldAnalyze);
		
		// Get from the URL request parameters the requested metrics
		boolean isQueryAll = params.getBool("queryAll", false);
		LOGGER.info("queryAll: " + isQueryAll);
		
		String[] metricsParams = params.getParams("metrics");
		
		if (metricsParams != null) {
			// Create the NamedList that will hold this component response
			NamedList<Object> antaMetricsNL = new NamedList<Object>();
			// Add this antaMetricsNL NamedList response to the response
			rb.rsp.add("antaMetrics", antaMetricsNL);

			for (String metrics : metricsParams) {
				//LOGGER.info("metrics: " + metrics);
				if (metrics.equalsIgnoreCase("tfidf")) {
					processTFIDF(antaMetricsNL, rb, isQueryAll);
				} else if (metrics.equalsIgnoreCase("gl")) {
					processGL(antaMetricsNL, rb, isQueryAll);
				} else {
					LOGGER.error("Unknown metrics: " + metrics);
				}
			}
		}

		// Request time
		long requestsTime = System.currentTimeMillis() - startTime;
		totalRequestsTime += requestsTime;
		LOGGER.info("### Ending process totalRequestsTime: " + requestsTime + " millisecondes");
	}

	private void processTFIDF(NamedList<Object> antaMetricsNL, ResponseBuilder rb, boolean isQueryAll) throws IOException {
		LOGGER.info("### Starting processTFIDF");
		long startTime = System.currentTimeMillis();
		
		LOGGER.info("### Starting processTFIDF global metrics");
		
		// NamedList for tfidf
		NamedList<Object> tfidfNL = new NamedList<Object>();
		antaMetricsNL.add("tfidf", tfidfNL);
		
		// Get the request Solr Index Searcher
		SolrIndexSearcher searcher = rb.req.getSearcher();
		
		// Get the request Index Reader as AtomicReader
		AtomicReader reader = searcher.getAtomicReader();
		
		// Number of documents that have at least one term for this field
		int docsCount = reader.getDocCount(fieldAnalyze);
		LOGGER.info("docsCount: " + String.valueOf(docsCount));
		tfidfNL.add("docsCount", docsCount);
		
		// All terms for this analyzed field
		Terms terms = reader.terms(fieldAnalyze);
		long termsCount = terms.size();
		LOGGER.info("termsCount: " + String.valueOf(termsCount));
		tfidfNL.add("termsCount", termsCount);

		// Sum of TermsEnum.totalTermFreq() for all terms in this analyzed field
		long termsFreqTotal = terms.getSumTotalTermFreq();
		LOGGER.info("termsFreqTotal: " + String.valueOf(termsFreqTotal));
		tfidfNL.add("termsFreqTotal", termsFreqTotal);
		
		long globalMetricsTime = System.currentTimeMillis();
		LOGGER.info("### Ending processTFIDF Global metrics: " + (globalMetricsTime - startTime) + " millisecondes");
		
		LOGGER.info("### Starting processTFIDF Documents iteration");
		
		// Querying documents
		DocList docList = null;
		
		if (isQueryAll) {
			Query queryAllDocs = new MatchAllDocsQuery();
			//docsSet = searcher.getDocSet(queryAllDocs, docsSet);
			Query filter = null;
			Sort sort = null;
			docList = searcher.getDocList(queryAllDocs, filter, sort, 0, MAX_RESULTS);
		} else {
			DocListAndSet listAndSet = rb.getResults();
			docList = listAndSet.docList;
		}

		if (docList == null) {
			LOGGER.info("Documents list is empty");
			
		} else {
			// docsListSize maybe different from docsCount
			// if some documents doesn't contains the fieldAnalyze
			int docsListSize = docList.size();
			LOGGER.info("docsListSize: " + docsListSize);
			
			// NamedList for documents list
			NamedList<Object> documentsNL = new NamedList<Object>();
			tfidfNL.add("documents", documentsNL);
			
			// Get a document iterator to look through all documents
			DocIterator docsIter = docList.iterator();

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
						long docTermsCount = termsVector.size();
						docNL.add("docTermsCount", docTermsCount);
						
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
							long termFreq = termsEnum.totalTermFreq();
							
							// termFreqAllDocs
							long termFreqAllDocs = reader.totalTermFreq(term);
							termMetrics.add("termFreqAllDocs", termFreqAllDocs);

							// tf
							// term frequency
							// basic : tf(t,d) = f(t,d)
							// scaled frequency : tf(t,d) = log (f(t,d) + 1)
							// augmented frequency : raw frequency divided by the maximum raw 
							// frequency of any term in the document
							// tf(t,d) = 0.5 + (0.5 * f(t,d)) / max(f(w,d) : w E d)
							termMetrics.add("tf", termFreq);

							// df
							// number of documents where the term t appears
							int df = reader.docFreq(term);
							termMetrics.add("df", df);

							// idf
							// inverse document frequency
							// log (docsCount / df)
							double idf = Math.log(docsCount / df);
							termMetrics.add("idf", idf);

							// tfidf
							// term frequency–inverse document frequency
							// tf * idf
							double tfidf = termFreq * idf;
							termMetrics.add("tfidf", tfidf);
						}
					}

				} catch (IOException ex) {
					LOGGER.error(null, ex);
				}
			}
		}
		long tfidfDuration = System.currentTimeMillis() - startTime;
		tfidfNL.add("requestTime", tfidfDuration);
		LOGGER.info("### Ending processTFIDF totalRequestsTime: " + tfidfDuration + " millisecondes");
	}

	private void processGL(NamedList<Object> antaMetricsNL, ResponseBuilder rb, boolean isQueryAll) throws IOException {
		LOGGER.info("### Starting processGL");
		long startTime = System.currentTimeMillis();
		
		
		LOGGER.info("### Starting MongoDB initialization");
		
		try {
			mongoClient = new MongoClient(mongodbHost , mongodbPort);
			mongoDB = mongoClient.getDB(mongodbDatabase);
			
			// Drop previous collections
			DBCollection infosCol = mongoDB.getCollection("infos");
			infosCol.drop();
			DBCollection termsCol = mongoDB.getCollection("terms");
			termsCol.drop();
			DBCollection coocurencesCol = mongoDB.getCollection("coocurences");
			coocurencesCol.drop();
			DBCollection holdsCol = mongoDB.getCollection("holds");
			holdsCol.drop();
			
			// Create collections with index
			infosCol = mongoDB.getCollection("infos");
			
			termsCol = mongoDB.getCollection("terms");
			termsCol.createIndex(new BasicDBObject("term", 1));
			
			coocurencesCol = mongoDB.getCollection("coocurences");
			coocurencesCol.createIndex(new BasicDBObject("i", 1));
			coocurencesCol.createIndex(new BasicDBObject("j", 1));
			
			holdsCol = mongoDB.getCollection("holds");
			holdsCol.createIndex(new BasicDBObject("id_sorted", 1));
			holdsCol.createIndex(new BasicDBObject("i", 1));
			holdsCol.createIndex(new BasicDBObject("j", 1));
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
				"Can't connect to MongoDB. Sart mongodb or verify your properties inside the antametrics searchComponent in the solrconfig.xml");
		}
		long mongoDbInitTime = System.currentTimeMillis();
		LOGGER.info("### Ending MongoDB initialization: " + (mongoDbInitTime - startTime) + " millisecondes");

		
		LOGGER.info("### Starting processGL Global metrics");
		// Create the NamedList that will hold basic gl response
		NamedList<Object> glNL = new NamedList<Object>();
		antaMetricsNL.add("gl", glNL);
		
		// Get the request Solr Index Searcher
		SolrIndexSearcher searcher = rb.req.getSearcher();
		
		// Get the request Index Reader as AtomicReader
		AtomicReader reader = searcher.getAtomicReader();
		
		// Number of documents that have at least one term for this field
		int docsCount = reader.getDocCount(fieldAnalyze);
		LOGGER.info("docsCount: " + docsCount);
		glNL.add("docsCount", docsCount);
		
		// All terms for this analyzed field
		Terms terms = reader.terms(fieldAnalyze);
		long termsCount = terms.size();
		LOGGER.info("termsCount: " + termsCount);
		glNL.add("termsCount", termsCount);

		// Sum of TermsEnum.totalTermFreq() for all terms in this analyzed field
		long termsFreqTotal = terms.getSumTotalTermFreq();
		LOGGER.info("termsFreqTotal: " + termsFreqTotal);
		glNL.add("termsFreqTotal", termsFreqTotal);
		
		long globalMetricsTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL Global metrics: " + (globalMetricsTime - mongoDbInitTime) + " millisecondes");

		
		LOGGER.info("### Starting processGL Terms filtering");

		// Minimum used for filtering
		glNL.add("minTermDocFreq", minTermDocFreq);
		LOGGER.info("minTermDocFreq: " + minTermDocFreq);
		glNL.add("minTermFreqAllDocs", minTermFreqAllDocs);
		LOGGER.info("minTermFreqAllDocs: " + minTermFreqAllDocs);

		// Create a TreeMap of Integer to store term occurrence frequency sorted by term
		TreeMap<String, TermMetrics> termsMap = new TreeMap<String, TermMetrics>();
		
		// TermsEnum for every terms
		TermsEnum termsEnum = terms.iterator(null);
		
		// Populate termsMap only with terms that above the minimums required
		BytesRef termBytesRef;
		
		while ((termBytesRef = termsEnum.next()) != null) {
			// termText : term text as UTF8 String
			String termText = termBytesRef.utf8ToString();
			
			// term reconstruction for querying
			Term term = new Term(fieldAnalyze, BytesRef.deepCopyOf(termBytesRef));
			
			// Ci = termDocFreq
			// occurrence frequency of the word Wi
			// how many documents mention the word Wi
			int termDocFreq = reader.docFreq(term);
			long termFreqAllDocs = reader.totalTermFreq(term);
			
			//StringBuffer debugSBTerm = new StringBuffer();
			//debugSBTerm.append("TermText: ");
			//debugSBTerm.append(termText);
			//debugSBTerm.append("termDocFreq: ");
			//debugSBTerm.append(termDocFreq);
			//debugSBTerm.append("termFreqAllDocs: ");
			//debugSBTerm.append(termFreqAllDocs);
			//LOGGER.info(debugSBTerm.toString());
			
			// Verify the minimum : minTermDocFreq, minTermFreqAllDocs
			if (termDocFreq > minTermDocFreq && termFreqAllDocs > minTermFreqAllDocs) {
				// Create a TermMetrics for storing the metrics of this term
				TermMetrics termMetrics = new TermMetrics();
				termMetrics.termQuery = new TermQuery(term);
				termMetrics.termFreqAllDocs = termFreqAllDocs;
				termMetrics.cBD = new BigDecimal(termDocFreq);
				termMetrics.cBD.setScale(BD_SCALE, BD_ROUND);

				termsMap.put(termText, termMetrics);
			}
		}
		int termsFilteredCount = termsMap.size();
		glNL.add("termsFilteredCount", String.valueOf(termsFilteredCount));
		LOGGER.info("termsFilteredCount: " + termsFilteredCount);
		
		long termsFilteringTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL Terms filtering: " + (termsFilteringTime - globalMetricsTime) + " millisecondes");
		
		
		LOGGER.info("### Starting processGL Co-occurrence and Hold metrics");
		
		// Get coocurences and holds collections from MongoDB
		DBCollection coocurencesCol = mongoDB.getCollection("coocurences");
		DBCollection holdsCol = mongoDB.getCollection("holds");
		
		BasicDBObject coocurenceQuery;
		DBObject coocurenceResp;
		BasicDBObject holdQuery;
		DBObject holdResp;
		
		for (Map.Entry<String, TermMetrics> entryI : termsMap.entrySet()) {
			//LOGGER.info("termI: " + entryI.getKey());
			
			for (Map.Entry<String, TermMetrics> entryJ : termsMap.entrySet()) {

				// Check if i ≠ j
				if (!entryI.getKey().equals(entryJ.getKey())) {
					//LOGGER.info("termJ: " + entryJ.getKey());
					
					// Search coocurences in MongoDB for Cij
					// using sorted i and j because Cij = Cji
					String[] keysSorted = {entryI.getKey(), entryJ.getKey()};
					java.util.Arrays.sort(keysSorted);
					
					coocurenceQuery = new BasicDBObject("i", keysSorted[0]).append("j", keysSorted[1]);
					coocurenceResp = coocurencesCol.findOne(coocurenceQuery);
					int cij;
					if (coocurenceResp == null) {
						// Search coocurences in MongoDB
						cij = searcher.numDocs(entryI.getValue().termQuery, entryJ.getValue().termQuery);
						coocurenceQuery.append("cij", cij);
						coocurencesCol.insert(coocurenceQuery);
					} else {
						cij = ((Integer)coocurenceResp.get("cij")).intValue();
					}
					
					// We store only positive value of Hij
					if (cij > 0) {
						// Search in MongoDB for Hij using a sorted identifier
						String id_sorted = keysSorted[0] + SEPARATOR + keysSorted[1];
						holdQuery = new BasicDBObject("id_sorted", id_sorted);
						holdResp = holdsCol.findOne(holdQuery);
						
						if (holdResp == null) {
							// Hi,j = Cij/Cj
							// hold of Wi on Wj
							// is the probability, for a document that mentions Wj, to mention Wi
							// ranges in [0;1]
							BigDecimal cijBD = new BigDecimal(cij);
							cijBD.setScale(BD_SCALE, BD_ROUND);
							
							BigDecimal hijBD = cijBD.divide(entryJ.getValue().cBD, BD_SCALE, BD_ROUND);
							BigDecimal hjiBD = cijBD.divide(entryI.getValue().cBD, BD_SCALE, BD_ROUND);
							//LOGGER.info("cij: " + cij + "; ci: " + entryI.getValue().cBD.toPlainString() + "; cj: " + entryJ.getValue().cBD.toPlainString() + "; hij: " + hijBD.toPlainString() + "; hji: " + hjiBD.toPlainString());
							
							holdQuery.append("i", entryI.getKey());
							holdQuery.append("j", entryJ.getKey());
							holdQuery.append("hij", hijBD.doubleValue());
							holdQuery.append("hji", hjiBD.doubleValue());
							
							holdsCol.insert(holdQuery);
						}
					}
				}
			}
			LOGGER.info("Ending cooccurrence and hold metrics for term: " + entryI.getKey());

		}
		long cooccurrenceTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL Co-occurrence and Hold metrics: " + (cooccurrenceTime - termsFilteringTime) + " millisecondes");
		
		
		LOGGER.info("### Starting processGL SVGL Metrics");
		
		// Constants
		BigDecimal termsFilteredCountMinus1BD = new BigDecimal(termsFilteredCount - 1);
		termsFilteredCountMinus1BD.setScale(BD_SCALE, BD_ROUND);
		
		// Variables
		DBCollection termsCol = mongoDB.getCollection("terms");
		
		BigDecimal sBD;
		BigDecimal vBD;
		BigDecimal gBD;
		BigDecimal lBD;
		
		BigDecimal sumGBD = BigDecimal.ZERO;
		sumGBD.setScale(BD_SCALE, BD_ROUND);
		BigDecimal sumLBD = BigDecimal.ZERO;
		sumGBD.setScale(BD_SCALE, BD_ROUND);
		
		for (Map.Entry<String, TermMetrics> entry : termsMap.entrySet()) {
			
			HoldMetrics sumHoldMetrics = calculateSum(entry.getKey(), holdsCol);
			
			// Si : mean hold strength of Wi
			// Si = Sum Hi,j where(i≠j) / ￼(n−1)
			// the probability, for a (randomly chosen) word,
			// to be mentioned in a document that mentions Wi
			sBD = sumHoldMetrics.hijBD.divide(termsFilteredCountMinus1BD, BD_SCALE, BD_ROUND);
			
			// Vi : mean hold vulnerability of Wi
			// Vi = Sum Hj,i where(j≠i) / ￼(n−1)
			// the probability, for Wi, to be mentioned in a document 
			// that mentions another (randomly chosen) word
			vBD = sumHoldMetrics.hjiBD.divide(termsFilteredCountMinus1BD, BD_SCALE, BD_ROUND);

			// Gi : genericity of Wi
			// Gi = Sum Hi,j - Hj,i where(j≠i) / ￼(n−1)
			// Gi = Si - Vi
			// the energy consumed by Wi
			// positive if Wi is generic, negative if Wi is specific
			gBD = sBD.subtract(vBD);


			// Li : locality of Wi
			// Li = Sum Hi,j + Hj,i where(j≠i) / ￼(n−1)
			// Li = Si + Vi
			// the coupling of Wi with other words
			// the energy exchanged by Wi
			lBD = sBD.add(vBD);
			
			// Sum of G and L
			sumGBD = sumGBD.add(gBD);
			sumLBD = sumLBD.add(lBD);
			
			BasicDBObject termMongo = new BasicDBObject();
			termMongo.append("term", entry.getKey());
			termMongo.append("termFreqAllDocs", entry.getValue().termFreqAllDocs);
			termMongo.append("c", entry.getValue().cBD.intValue());
			termMongo.append("s", sBD.doubleValue());
			termMongo.append("v", vBD.doubleValue());
			termMongo.append("g", gBD.doubleValue());
			termMongo.append("l", lBD.doubleValue());
			termsCol.insert(termMongo);
		}
		// sumG
		glNL.add("sumG", sumGBD.toPlainString());
		LOGGER.info("sumG: " + sumGBD.toPlainString());
		
		// sumLdivide2
		BigDecimal twoBD = new BigDecimal(2);
		twoBD.setScale(BD_SCALE, BD_ROUND);
		BigDecimal sumLdivide2BD = sumLBD.divide(twoBD, BD_SCALE, BD_ROUND);
		glNL.add("sumLdivide2", sumLdivide2BD.toPlainString());
		LOGGER.info("sumLdivide2: " + sumLdivide2BD.toPlainString());
		
		// Save Global metrics to MongoDB infos collection
		DBCollection infosCol = mongoDB.getCollection("infos");
		BasicDBObject infosMongo = new BasicDBObject("docsCount", docsCount);
		infosMongo.append("termsCount", termsCount);
		infosMongo.append("termsFreqTotal", termsFreqTotal);
		infosMongo.append("termsFilteredCount", termsFilteredCount);
		infosMongo.append("minTermDocFreq", minTermDocFreq);
		infosMongo.append("minTermFreqAllDocs", minTermFreqAllDocs);
		infosMongo.append("sumG", sumGBD.doubleValue());
		infosMongo.append("sumLdivide2", sumLdivide2BD.doubleValue());
		infosCol.insert(infosMongo);
		
		// Close MongoDB client
		mongoClient.close();
		
		long svglTime = System.currentTimeMillis();
		LOGGER.info("### Ending processGL SVGL Metrics: " + (svglTime - cooccurrenceTime) + " millisecondes");
		
		long glDuration = System.currentTimeMillis() - startTime;
		glNL.add("requestTime", String.valueOf(glDuration));
		LOGGER.info("### Ending processGL totalRequestsTime: " + glDuration + " millisecondes");
	}

	private HoldMetrics calculateSum(String termText, DBCollection holds) {
		DBObject clause1 = new BasicDBObject("i", termText);  
		DBObject clause2 = new BasicDBObject("j", termText);    
		BasicDBList or = new BasicDBList();
		or.add(clause1);
		or.add(clause2);
		DBObject holdQuery = new BasicDBObject("$or", or);
		DBCursor holdsCursor = holds.find(holdQuery);
		HoldMetrics sum = new HoldMetrics();
		BigDecimal hijBD;
		BigDecimal hjiBD;
		//int count = 0;
		try {
			while (holdsCursor.hasNext()) {
				//count++;
				DBObject hold = holdsCursor.next();
				if (hold.get("i").equals(termText)) {
					hijBD = new BigDecimal((Double)hold.get("hij"));
					hijBD.setScale(BD_SCALE, BD_ROUND);
					hjiBD = new BigDecimal((Double)hold.get("hji"));
					hjiBD.setScale(BD_SCALE, BD_ROUND);
					//LOGGER.info("i=termText hijBD: " + hijBD.toPlainString() + " hjiBD: " + hjiBD.toPlainString());
					sum.hijBD = sum.hijBD.add(hijBD);
					sum.hjiBD = sum.hjiBD.add(hjiBD);
				} else if (hold.get("j").equals(termText))  {
					hijBD = new BigDecimal((Double)hold.get("hji"));
					hijBD.setScale(BD_SCALE, BD_ROUND);
					hjiBD = new BigDecimal((Double)hold.get("hij"));
					hjiBD.setScale(BD_SCALE, BD_ROUND);
					//LOGGER.info("j=termText hijBD: " + hijBD.toPlainString() + " hjiBD: " + hjiBD.toPlainString());
					sum.hijBD = sum.hijBD.add(hijBD);
					sum.hjiBD = sum.hjiBD.add(hjiBD);
				} else {
					LOGGER.info("ERROR : termText different from i and j in hold");
				}
			}
		} finally {
			holdsCursor.close();
		}
		//LOGGER.info("calculateSum for term: " + termText + " count: " + count + " SumHij: " + sum.hijBD.toPlainString() + " SumHji: " + sum.hjiBD.toPlainString());
		return sum;
	}

	// //////////////////////////////////
	// NamedListInitializedPlugin methods
	// //////////////////////////////////

	public void finishStage() {
		mongoClient.close();
	}

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
		return "0.1.0";
	}

	@Override
	public NamedList<Object> getStatistics() {
		// For the statistics panel in Solr
		NamedList<Object> all = new SimpleOrderedMap<Object>();
		all.add("requests", "" + numRequests);
		all.add("totalRequestTime(ms)", "" + totalRequestsTime);
		return all;
	}
}
