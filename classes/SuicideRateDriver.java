import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SuicideRateDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Suicide Rate Analysis");

        job.setJarByClass(SuicideRateDriver.class);
        job.setMapperClass(SuicideRateMapper.class);
        job.setReducerClass(SuicideRateReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input CSV
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output dir

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
