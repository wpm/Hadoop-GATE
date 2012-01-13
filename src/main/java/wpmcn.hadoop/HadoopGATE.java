package wpmcn.hadoop;

import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.util.GateException;
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

import java.io.IOException;

/**
 * A Hadoop job that runs GATE applications
 *
 * This job runs a GATE application on text. The GATE application is a archive file with an application.xgapp file in
 * its root. This application is copied to HDFS and placed into the distributed cache.
 */
public class HadoopGATE extends Configured implements Tool {
   private static final String HDFS_GATE_APP = "/tmp/gate-app.zip";

   static public class HadoopGATEMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
      static private GATEApplication gate;
      private Text annotation = new Text();

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
         if (null != gate) {
            Configuration configuration = context.getConfiguration();
            Path[] localCache = DistributedCache.getLocalCacheArchives(configuration);
            try {
               gate = new GATEApplication(localCache[0].toString());
            } catch (GateException e) {
               throw new RuntimeException(e);
            }
         }
      }

      @Override
      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
         String xml;
         try {
            xml = gate.xmlAnnotation(text.toString());
         } catch (ResourceInstantiationException e) {
            throw new RuntimeException(e);
         } catch (ExecutionException e) {
            throw new RuntimeException(e);
         }
         annotation.set(xml);
         context.write(offset, annotation);
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
         gate.close();
      }
   }

   static public Job createJob(Configuration configuration, String[] args) throws IOException {
      Path gateApp = new Path(args[0]);
      Path input = new Path(args[1]);
      Path output = new Path(args[2]);
      // Put the GATE application into the distributed cache.
      FileSystem fs = FileSystem.get(configuration);
      Path hdfsGateApp = new Path(HDFS_GATE_APP);
      // TODO Copy to a random temp directory so that there are no name conflicts.
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
