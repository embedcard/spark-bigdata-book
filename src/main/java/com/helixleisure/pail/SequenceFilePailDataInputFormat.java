package com.helixleisure.pail;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailFormatFactory;
import com.backtype.hadoop.pail.PailInputSplit;
import com.backtype.support.Utils;
import com.helixleisure.schema.Data;

public class SequenceFilePailDataInputFormat<T> extends SequenceFileInputFormat<Text, Data> {
    private Pail _currPail;


    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        List<InputSplit> ret = new ArrayList<InputSplit>();
        Path[] roots = FileInputFormat.getInputPaths(job);
        for(int i=0; i < roots.length; i++) {
            _currPail = new Pail(roots[i].toString());
            InputSplit[] splits = super.getSplits(job, numSplits);
            for(InputSplit split: splits) {
                ret.add(new PailInputSplit(_currPail.getFileSystem(), _currPail.getInstanceRoot(), _currPail.getSpec(), job, (FileSplit) split));
            }
        }
        return ret.toArray(new InputSplit[ret.size()]);
    }

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        List<Path> paths = PailFormatFactory.getPailPaths(_currPail, job);
        FileSystem fs = _currPail.getFileSystem();
        FileStatus[] ret = new FileStatus[paths.size()];
        for(int i=0; i<paths.size(); i++) {
            ret[i] = fs.getFileStatus(paths.get(i).makeQualified(fs));
        }
        return ret;
    }

    @Override
    public RecordReader<Text, Data> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new SequenceFilePailThriftRecordReader(job, (PailInputSplit) split, reporter);
    }
    
    public static class SequenceFilePailThriftRecordReader implements RecordReader<Text, Data> {
        private static Logger LOG = LoggerFactory.getLogger(SequenceFilePailThriftRecordReader.class);
        public static final int NUM_TRIES = 10;

        JobConf conf;
        PailInputSplit split;
        int recordsRead;
        Reporter reporter;
        protected static final transient TDeserializer DES = new TDeserializer();

        SequenceFileRecordReader<BytesWritable, NullWritable> delegate;


        public SequenceFilePailThriftRecordReader(JobConf conf, PailInputSplit split, Reporter reporter) throws IOException {
           this.split = split;
           this.conf = conf;
           this.recordsRead = 0;
           this.reporter = reporter;
           if (LOG.isInfoEnabled()) {
        	   LOG.info("Processing pail file {}",split.getPath().toString());
           }
           resetDelegate();
        }

        private void resetDelegate() throws IOException {
           this.delegate = new SequenceFileRecordReader<BytesWritable, NullWritable>(conf, split);
           BytesWritable dummyValue = new BytesWritable();
           for(int i=0; i<recordsRead; i++) {
               delegate.next(dummyValue, NullWritable.get());
           }
        }

        private void progress() {
            if(reporter!=null) {
                reporter.progress();
            }
        }

        public boolean next(Text k, Data v) throws IOException {
            /**
             * There's 2 bugs that happen here, both resulting in indistinguishable EOFExceptions.
             *
             * 1. Random EOFExceptions when reading data off of S3. Usually succeeds on the 2nd try.
             * 2. Corrupted files most likely due to network corruption (which isn't handled by Hadoop/S3 integration).
             *    These always result in error.
             *
             * The strategy is to retry a few times. If it fails every time then we're in case #2, and the best thing we can do
             * is continue on and accept the data loss. If we're in case #1, it'll just succeed.
             */
            for(int i=0; i<NUM_TRIES; i++) {
                try {
                	BytesWritable dummyValue = new BytesWritable();
                    boolean ret = delegate.next(dummyValue, NullWritable.get());
                    k.set(split.getPailRelPath());
                    recordsRead++;
                    // if the delegate processed the value, we can parse it to our data unit
                    if (ret) {
                    	try {
							DES.deserialize(v, dummyValue.getBytes());
						} catch (TException e) {
							throw new IOException(e);
						}
                    }
                    return ret;
                } catch(EOFException e) {
                    progress();
                    Utils.sleep(10000); //in case it takes time for S3 to recover
                    progress();
                    //this happens due to some sort of S3 corruption bug.
                    LOG.error("Hit an EOF exception while processing file " + split.getPath().toString() +
                              " with records read = " + recordsRead);
                    resetDelegate();
                }
            }
            //stop trying to read the file at this point and discard the rest of the file
            return false;
        }

        public Text createKey() {
            return new Text();
        }

        public Data createValue() {
            return new Data();
        }

        public long getPos() throws IOException {
            return delegate.getPos();
        }

        public void close() throws IOException {
            delegate.close();
        }

        public float getProgress() throws IOException {
            return delegate.getProgress();
        }

    }
}
