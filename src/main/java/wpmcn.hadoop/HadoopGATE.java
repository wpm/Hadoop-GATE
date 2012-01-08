package wpmcn.hadoop;

import gate.*;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.util.GateException;
import gate.util.persistence.PersistenceManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class HadoopGATE extends Configured implements Tool {
   private static final String HDFS_GATE_APP = "/tmp/gate-app.zip";

   static public class HadoopGATEMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
      private final String APPLICATION_XGAPP = "application.xgapp";
      private Path gateApp;
      private Corpus corpus;
      private CorpusController application;
      private Text annotation = new Text();

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
         Configuration configuration = context.getConfiguration();
         Path[] localCache = DistributedCache.getLocalCacheArchives(configuration);
         gateApp = new Path(localCache[0], APPLICATION_XGAPP);
         try {
            Gate.init();
            application = (CorpusController) PersistenceManager.loadObjectFromFile(new File(gateApp.toString()));
            corpus = Factory.newCorpus("Hadoop Corpus");
            application.setCorpus(corpus);
         } catch (GateException e) {
            throw new RuntimeException(e);
         }
      }

      @Override
      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
         context.setStatus(gateApp.toString());
         System.out.println(gateApp + ": <" + offset + " " + text + ">");

         Document document = annotateDocument(text.toString());
         annotation.set(document.toXml());
         context.write(offset, annotation);
      }

      @SuppressWarnings({"unchecked"}) // Needed for corpus.add(document)
      private Document annotateDocument(String contents) {
         Document document;
         try {
            document = Factory.newDocument(contents);
            corpus.add(document);
            application.execute();
         } catch (ResourceInstantiationException e) {
            throw new RuntimeException(e);
         } catch (ExecutionException e) {
            throw new RuntimeException(e);
         }
         Factory.deleteResource(document);
         corpus.clear();
         return document;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
         Factory.deleteResource(corpus);
         Factory.deleteResource(application);
      }
   }

   static public Job createJob(Configuration configuration, String[] args) throws IOException {
      Path gateApp = new Path(args[0]);
      Path input = new Path(args[1]);
      Path output = new Path(args[2]);
      // Put the GATE application into the distributed cache.
      FileSystem fs = FileSystem.get(configuration);
      Path hdfsGateApp = new Path(HDFS_GATE_APP);
      fs.copyFromLocalFile(gateApp, hdfsGateApp);
      DistributedCache.addCacheArchive(hdfsGateApp.toUri(), configuration);

      Job job = new Job(configuration, "GATE " + output);
      FileInputFormat.addInputPath(job, input);
      SequenceFileOutputFormat.setOutputPath(job, output);

      job.setJarByClass(HadoopGATE.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setMapperClass(HadoopGATEMapper.class);

      return job;
   }

   public int run(String[] args) throws Exception {
      Job job = createJob(getConf(), args);
      boolean success = job.waitForCompletion(true);
      if (success)
         FileSystem.get(job.getConfiguration()).deleteOnExit(new Path(HDFS_GATE_APP));
      return success ? 0 : -1;
   }

   static public void main(String[] args) throws Exception {
      System.exit(ToolRunner.run(new HadoopGATE(), args));
   }
}
