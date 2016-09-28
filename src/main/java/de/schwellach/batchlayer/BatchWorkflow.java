package de.schwellach.batchlayer;


import static de.schwellach.test.Data.makeEquiv;
import static de.schwellach.test.Data.makePageview;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.Mode;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.helixleisure.schema.Data;
import com.helixleisure.schema.DataUnit;
import com.helixleisure.schema.EquivEdge;
import com.helixleisure.schema.PageID;
import com.helixleisure.schema.PageViewEdge;
import com.helixleisure.schema.PersonID;

import de.schwellach.pail.DataPailStructure;
import de.schwellach.pail.SequenceFilePailDataInputFormat;
import de.schwellach.pail.SplitDataPailStructure;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/**
 * Adaption of the batch workflow from Nathan Marz (http://twitter.com/nathanmarz) from his book
 * https://www.manning.com/books/big-data
 * 
 * @author Janos Schwellach (http://twitter.com/jschwellach)
 */
public class BatchWorkflow {
	private static final Logger LOG = LoggerFactory.getLogger(BatchWorkflow.class);
	
	public static final String ROOT = "/tmp/swaroot/";
	public static final String DATA_ROOT = ROOT + "data/";
	public static final String OUTPUTS_ROOT = ROOT + "outputs/";
	public static final String MASTER_ROOT = DATA_ROOT + "master";
	public static final String NEW_ROOT = DATA_ROOT + "new";

	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public void initTestData() throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(DATA_ROOT), true);
        fs.delete(new Path(OUTPUTS_ROOT), true);
        fs.mkdirs(new Path(DATA_ROOT));
        fs.mkdirs(new Path(OUTPUTS_ROOT + "edb"));

        @SuppressWarnings("unused")
		Pail masterPail = Pail.create(MASTER_ROOT, new SplitDataPailStructure());
        Pail<Data> newPail = Pail.create(NEW_ROOT, new DataPailStructure());

		TypedRecordOutputStream os = newPail.openWrite();
        os.writeObject(makePageview(1, "http://foo.com/post1", 60));
        os.writeObject(makePageview(3, "http://foo.com/post1", 62));
        os.writeObject(makePageview(1, "http://foo.com/post1", 4000));
        os.writeObject(makePageview(1, "http://foo.com/post2", 4000));
        os.writeObject(makePageview(1, "http://foo.com/post2", 10000));
        os.writeObject(makePageview(5, "http://foo.com/post3", 10600));
        os.writeObject(makeEquiv(1, 3));
        os.writeObject(makeEquiv(3, 5));

        os.writeObject(makePageview(2, "http://foo.com/post1", 60));
        os.writeObject(makePageview(2, "http://foo.com/post3", 62));

        os.close();

    }
	
    private SparkConf conf;

	public void init() {
		conf = new SparkConf();
		// we need to add the following two lines to be able to serialize the
		// java classes of hadoop.
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryo.classesToRegister", "org.apache.hadoop.io.Text")
				.set("spark.kryo.classesToRegister", "com.backtype.hadoop.pail.Pail")
				.set("spark.kryoserializer.buffer.max value", "2g");
		conf.setMaster("local");
		conf.setAppName("batch-workflow");
	}
	

	@SuppressWarnings("rawtypes")
	public void ingest(Pail masterPail, Pail newDataPail) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("/tmp/swa"),true);
		fs.mkdirs(new Path("/tmp/swa"));
		
		Pail snapshotPail = newDataPail.snapshot("/tmp/swa/newDataSnapshot");
		appendNewDataToMasterDataPail(masterPail, snapshotPail);
		newDataPail.deleteSnapshot(snapshotPail);
		masterPail.consolidate();
	}
	
	@SuppressWarnings("rawtypes")
	private void appendNewDataToMasterDataPail(Pail masterPail, Pail snapshotPail) throws IOException {
		Pail shreddedPail = shred();
		masterPail.absorb(shreddedPail);
	}
	
	
	// this needs to make sure we convert the data in case the data structures are different.
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Pail shred() throws IOException {
		Pail source = new Pail(Mode.SPARK,"/tmp/swa/newDataSnapshot");
		String sinkFolder = "/tmp/swa/shredded";
		Pail sink = Pail.create(Mode.SPARK, "/tmp/swa/shredded", new SplitDataPailStructure());
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
		
		JavaPairRDD<Text,Data> hadoopFile = jsc.hadoopFile(source.getInstanceRoot(), SequenceFilePailDataInputFormat.class, Text.class, Data.class,0);
		hadoopFile.foreach(f-> {
			Pail s = new Pail(Mode.SPARK,sinkFolder);
			TypedRecordOutputStream stream = s.openWrite();
			stream.writeObject(f._2);
			stream.close();
		});
		jsc.close();
		return sink;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void normalizeURLs() throws IOException {
		Pail masterDataset = new Pail(Mode.SPARK, DATA_ROOT+"master");
		String outFolder = "/tmp/swa/normalized_urls";
		Pail.create(Mode.SPARK, "/tmp/swa/normalized_urls", new SplitDataPailStructure());
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
		
		JavaPairRDD<Text,Data> hadoopFile = jsc.hadoopFile(masterDataset.getInstanceRoot(), SequenceFilePailDataInputFormat.class, Text.class, Data.class,0);
		JavaRDD<Data> map = hadoopFile.map(new NormalizeURL());
		map.foreach(f-> {
			if (LOG.isDebugEnabled()) {
				LOG.debug("serializing {}",f);
			}
			Pail out = new Pail(Mode.SPARK,outFolder);
			TypedRecordOutputStream stream = out.openWrite();
			stream.writeObject(f);
			stream.close();
		});
	}
	
	
	public static class NormalizeURL implements Function<Tuple2<Text,Data>, Data> {
		private static final long serialVersionUID = 1899384619952957749L;
		private static final Logger LOG = LoggerFactory.getLogger(NormalizeURL.class);

		@Override
		public Data call(Tuple2<Text, Data> value) throws Exception {
			Data data = value._2.deepCopy();
			DataUnit du = data.getDataunit();
			if (du.getSetField() == DataUnit._Fields.PAGE_VIEW) {
				normalize(du.getPageView().getPage());
			} else if (du.getSetField() == DataUnit._Fields.PAGE_PROPERTY) {
				normalize(du.getPageProperty().getId());
			}
			return data;
		}
		
		private void normalize(PageID page) {
			if (page.getSetField() == PageID._Fields.URL) {
				String urlStr = page.getUrl();
				try {
					URL url = new URL(urlStr);
					page.setUrl(url.getProtocol() + "://" +url.getHost() + url.getPath());
					if (LOG.isDebugEnabled()) {
						LOG.debug("normalized url from {} to {}",urlStr,page.getUrl());
					}
				} catch (MalformedURLException e) {
				}
			}
		}
	}
	
	/*
	 * Normalizing the user ID's. User ID's belonging to the same person are marked as equivs
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void normalizeUserIds() throws IOException {
		Pail equivs = new Pail(Mode.SPARK, DATA_ROOT+"master").getSubPail(DataUnit._Fields.EQUIV.getThriftFieldId());
		String output = "/tmp/swa/equivs0";
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
		JavaPairRDD<Text,Data> hadoopFile = jsc.hadoopFile(equivs.getInstanceRoot(), SequenceFilePailDataInputFormat.class, Text.class, Data.class,0);

		JavaRDD<Tuple2<PersonID,PersonID>> map = hadoopFile.map(f->{
			Data data = f._2;
			EquivEdge equiv = data.getDataunit().getEquiv();
			return new Tuple2<>(equiv.getId1(),equiv.getId2());
		});
		map.saveAsObjectFile(output);
		int i = 1;
		while(true) {
			JavaRDD<Tuple2<PersonID, PersonID>> progressEdges = runUserIdNormalizationIteration(jsc,i);
			if (progressEdges.count()==0) {
				break;
			}
			i++;
		}
		
		Pail pageviewsPail = new Pail(Mode.SPARK,"/tmp/swa/normalized_urls").getSubPail(DataUnit._Fields.PAGE_VIEW.getThriftFieldId());
		JavaPairRDD<Text,Data> pageviews = jsc.hadoopFile(pageviewsPail.getInstanceRoot(), SequenceFilePailDataInputFormat.class, Text.class, Data.class,0);
		JavaRDD<Tuple2<PersonID,PersonID>> newIds = jsc.objectFile("/tmp/swa/equivs"+i);
		String resultFolder = "/tmp/swa/normalized_pageview_users";
		Pail.create(Mode.SPARK,resultFolder, new SplitDataPailStructure());
		
		pageviews.foreach(f->LOG.debug(f.toString()));
		newIds.foreach(f->LOG.debug(f.toString()));
		
		JavaPairRDD<PersonID, PersonID> newIdsPerson = newIds.mapToPair(f-> {
			return new Tuple2<>(f._2,f._1);
		});
		
		JavaPairRDD<PersonID,Data> pageviewsPair = pageviews.mapToPair(f->{
			Data data = f._2;
			PersonID person = data.getDataunit().getPageView().getPerson();
			return new Tuple2<>(person,data);
		});
		JavaPairRDD<PersonID,Tuple2<Data,Optional<PersonID>>> leftOuterJoin = pageviewsPair.leftOuterJoin(newIdsPerson);
		leftOuterJoin.foreach(f->LOG.debug(f.toString()));
		
		JavaRDD<Data> normalizedPageview = leftOuterJoin.map(f-> {
			Tuple2<Data,Optional<PersonID>> tuple2 = f._2();
			Data data = tuple2._1();
			if (tuple2._2().isPresent()) {
				data.getDataunit().getPageView().setPerson(tuple2._2.get());
			}
			return data;
		});
		
		normalizedPageview.foreach(f->{
			if (LOG.isDebugEnabled()) {
				LOG.debug("serializing {}",f);
			}
			Pail out = new Pail(Mode.SPARK,resultFolder);
			TypedRecordOutputStream stream = out.openWrite();
			stream.writeObject(f);
			stream.close();
		});
	}
	
	@SuppressWarnings("unchecked")
	private JavaRDD<Tuple2<PersonID, PersonID>> runUserIdNormalizationIteration(JavaSparkContext jsc, int i) {
		String output = "/tmp/swa/equivs"+i;
		JavaRDD<Object> objectFile = jsc.objectFile("/tmp/swa/equivs"+ (i-1));

		// Bidirectional Edges
		JavaRDD<Tuple2<PersonID,PersonID>> flatMap = objectFile.flatMap(f-> {
			List<Tuple2<PersonID,PersonID>> result = new ArrayList<>();
			Tuple2<PersonID, PersonID> tuple = (Tuple2<PersonID, PersonID>) f;
			PersonID node1 = tuple._1;
			PersonID node2 = tuple._2;
			if (!node1.equals(node2)) {
				result.add(	new Tuple2<PersonID,PersonID>(node1, node2) );
				result.add(	new Tuple2<PersonID,PersonID>(node2, node1) );
			}
			return result.iterator();
		});

		// grouping by node1 to get all results where it belongs to
		JavaPairRDD<PersonID,Iterable<Tuple2<PersonID,PersonID>>> groupBy = flatMap.groupBy(f-> f._1);
		
		// Iterate over the edges and introduce new ones
		JavaRDD<Tuple3<PersonID, PersonID, Boolean>> iteration = groupBy.flatMap(f-> {
			PersonID grouped = f._1;
			Iterator<Tuple2<PersonID, PersonID>> iterable = f._2().iterator();
			TreeSet<PersonID> allIds = new TreeSet<PersonID>();
			allIds.add(grouped);
			while(iterable.hasNext()) {
				allIds.add(iterable.next()._2);
			}
			
			Iterator<PersonID> allIdsIt = allIds.iterator();
			PersonID smallest = allIdsIt.next();
			boolean isProgress = allIds.size() > 2 && ! grouped.equals(smallest);
			
			List<Tuple3<PersonID,PersonID,Boolean>> result = new ArrayList<>();
			while(allIdsIt.hasNext()) {
				PersonID id = allIdsIt.next();
				result.add(new Tuple3<>(smallest, id, isProgress));
			}
			return result.iterator();
		}).distinct();
		
		JavaRDD<Tuple2<PersonID, PersonID>> newEdgeSet = iteration.map(f->new Tuple2<PersonID,PersonID>(f._1(),f._2())).distinct();
		newEdgeSet.saveAsObjectFile(output);
		
		JavaRDD<Tuple2<PersonID, PersonID>> progressEdges = iteration.filter(f->f._3().equals(Boolean.TRUE)).map(f-> {
			return new Tuple2<PersonID,PersonID>(f._1(),f._2());
		});
		return progressEdges;
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void deduplicatePageviews() throws IOException {
		String src = "/tmp/swa/normalized_pageview_users";
		String out = "/tmp/swa/unique_pageviews";

		Pail source = new Pail(Mode.SPARK,src).getSubPail(DataUnit._Fields.PAGE_VIEW.getThriftFieldId());
		Pail.create(Mode.SPARK,out,new SplitDataPailStructure());
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
		JavaPairRDD<Text,Data> pageviews = jsc.hadoopFile(source.getInstanceRoot(), SequenceFilePailDataInputFormat.class, Text.class, Data.class,0);

		JavaRDD<Data> distinct = pageviews.map(f->f._2).distinct().sortBy(f->f, true, 0);
		distinct.foreach(f->{
			if (LOG.isDebugEnabled()) {
				LOG.debug("serializing {}",f);
			}
			Pail output = new Pail(Mode.SPARK,out);
			TypedRecordOutputStream stream = output.openWrite();
			stream.writeObject(f);
			stream.close();
		});
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public JavaRDD<Tuple4<String,String,Integer,Long>> pageviewBatchView() throws IOException{
		Pail sourcePail = new Pail(Mode.SPARK,"/tmp/swa/unique_pageviews");
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
		JavaPairRDD<Text,Data> pageviews = jsc.hadoopFile(sourcePail.getInstanceRoot(), SequenceFilePailDataInputFormat.class, Text.class, Data.class,0);

		// hourly rollup
		// we could just return a Tuple2 for our calculation but to follow the code from the book we do that in the next step
		JavaRDD<Tuple3<String,Long,Integer>> map = pageviews.map(f->{
			final int HOUR_SECS = 60 * 60;
			Data data = f._2();
			PageViewEdge pageview = data.getDataunit().getPageView();
			if (pageview.getPage().getSetField() == PageID._Fields.URL) {
				int hourBucket = data.getPedigree().getTrueAsOfSecs() / HOUR_SECS;
				return new Tuple3<>(pageview.getPage().getUrl(), pageview.getPerson().getUserId(), hourBucket);
			}
			return null;
		});
		map.foreach(f->LOG.debug(f.toString()));

		JavaRDD<Tuple2<String, Integer>> visits = map.map(f->new Tuple2<>(f._1(),f._3()));
		JavaPairRDD<Tuple2<String, Integer>, Long> hourlyRollupPairs = visits.mapToPair(f-> new Tuple2<>(f,1l)).reduceByKey((v1,v2)->v1+v2);
		JavaRDD<Tuple3<String, Integer, Long>> hourlyRollup = hourlyRollupPairs.map(f-> new Tuple3<>(f._1._1,f._1._2,f._2));
		
		// emiting the granularities
		JavaPairRDD<Tuple3<String, Integer, String>, Long> granularities = hourlyRollup.flatMapToPair(f-> {
			int hourBucket = f._2();
			int dayBucket = hourBucket / 24;
			int weekBucket = dayBucket / 7;
			int monthBucket = dayBucket / 28;
			
			List<Tuple2<Tuple3<String,Integer,String>,Long>> result = new ArrayList<>();
			result.add(new Tuple2<>(new Tuple3<>("h", hourBucket, f._1()), f._3()));
			result.add(new Tuple2<>(new Tuple3<>("d", dayBucket, f._1()), f._3()));
			result.add(new Tuple2<>(new Tuple3<>("w", weekBucket, f._1()), f._3()));
			result.add(new Tuple2<>(new Tuple3<>("m", monthBucket, f._1()), f._3()));
			return result.iterator();
		});
		granularities = granularities.reduceByKey((v1,v2)->v1+v2);
		JavaRDD<Tuple4<String, String, Integer, Long>> map2 = granularities.map(f->
			new Tuple4<>(f._1._3(),f._1._1(),f._1._2(),f._2)
		).sortBy(f->f._2(), true, 0);
		// adding cache for later usage
		map2.cache();
		map2.foreach(f->LOG.debug("{}",f));
		return map2;
	}
	

	@SuppressWarnings("rawtypes")
	public void batchWorkflow() throws IOException {
		init();
		
		Pail masterPail = new Pail(Mode.SPARK, MASTER_ROOT);
		Pail newDataPail = new Pail(Mode.SPARK, NEW_ROOT);
		
		ingest(masterPail, newDataPail);
		normalizeURLs();
		normalizeUserIds();
		deduplicatePageviews();
		pageviewBatchView();
	}
	

	public static void main(String[] args) throws Exception {
		BatchWorkflow bw = new BatchWorkflow();
		bw.initTestData();
		bw.batchWorkflow();
	}
}
