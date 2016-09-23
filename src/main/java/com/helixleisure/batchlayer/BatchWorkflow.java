package com.helixleisure.batchlayer;


import static com.helixleisure.test.Data.makeEquiv;
import static com.helixleisure.test.Data.makePageview;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.Mode;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.helixleisure.pail.DataPailStructure;
import com.helixleisure.pail.SequenceFilePailDataInputFormat;
import com.helixleisure.pail.SplitDataPailStructure;
import com.helixleisure.schema.Data;
import com.helixleisure.schema.DataUnit;
import com.helixleisure.schema.EquivEdge;
import com.helixleisure.schema.PageID;

import scala.Tuple2;


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

        @SuppressWarnings("rawtypes")
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
		// java classes of hadoop. This list is comma seperated
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
		// TODO: check if there is a more efficient way to do it
		hadoopFile.foreach(f-> {
			Pail s = new Pail(Mode.SPARK,sinkFolder);
			TypedRecordOutputStream stream = s.openWrite();
			stream.writeObject(f._2);
			stream.close();
		});
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
		// TODO: checking if we can use spark methodes (map.saveAsTextFile) instead
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
	
	public void normalizeUserIds() throws IOException {
		Pail equivs = new Pail(Mode.SPARK, DATA_ROOT+"master").getSubPail(DataUnit._Fields.EQUIV.getThriftFieldId());
		String output = "/tmp/swa/equivs0";
		
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
		JavaPairRDD<Text,Data> hadoopFile = jsc.hadoopFile(equivs.getInstanceRoot(), SequenceFilePailDataInputFormat.class, Text.class, Data.class,0);

		
		JavaRDD<Tuple2> map = hadoopFile.map(f->{
			Data data = f._2;
			EquivEdge equiv = data.getDataunit().getEquiv();
			return new Tuple2(equiv.getId1(),equiv.getId2());
		});
		map.saveAsTextFile(output);
		int i = 1;
		while(true) {
			runUserIdNormalizationIteration(i);
			
		}
		
	}
	
	private void runUserIdNormalizationIteration(int i) {
		
	}
	
	

	@SuppressWarnings("rawtypes")
	public void batchWorkflow() throws IOException {
		init();
		
		Pail masterPail = new Pail(Mode.SPARK, MASTER_ROOT);
		Pail newDataPail = new Pail(Mode.SPARK, NEW_ROOT);
		
		ingest(masterPail, newDataPail);
		normalizeURLs();
		normalizeUserIds();
	}
	
	public static void main(String[] args) throws Exception {
		BatchWorkflow bw = new BatchWorkflow();
		bw.initTestData();
		bw.batchWorkflow();
	}
}
