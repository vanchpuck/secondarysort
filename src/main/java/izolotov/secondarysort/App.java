package izolotov.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.StreamSupport;

public class App {

    public static class HostMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Set<String> blacklist;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            URI cacheFileUri =  Arrays.stream(context.getCacheFiles())
                    .filter(path -> new Path(path.getPath()).getName().equals("blacklist.txt")).findFirst().get();
            blacklist = new HashSet<>(Files.readAllLines(Paths.get(cacheFileUri.getPath())));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String host = new URL(value.toString()).getHost();
            if(!blacklist.contains(host)) {
                context.write(new Text(host), new IntWritable(1));
            }
        }
    }

    public static class SimpleHostCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = StreamSupport.stream(values.spliterator(), false).mapToInt(val -> val.get()).sum();
            context.write(new Text(key.toString()), new IntWritable(sum));
        }
    }

    public static class SortingMapper extends Mapper<Text, Text, IntWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int v = Integer.valueOf(value.toString());
            context.write(new IntWritable(v), key);
        }
    }

    public static class SortingReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text host : values) {
                context.write(host, key);
            }
        }
    }

    public static class DescendingKey extends WritableComparator {
        public DescendingKey() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -key1.compareTo(key2);
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();


            Job job = Job.getInstance(conf, "AggregateByHostJob");
            job.setJarByClass(App.class);
            job.setMapperClass(HostMapper.class);
            job.setReducerClass(SimpleHostCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.addCacheFile(new URI(args[3]));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            Job sortingJob = Job.getInstance(conf, "SortingJob");
            sortingJob.setJarByClass(App.class);
            sortingJob.setMapperClass(SortingMapper.class);
            sortingJob.setReducerClass(SortingReducer.class);
            sortingJob.setMapOutputKeyClass(IntWritable.class);
            sortingJob.setMapOutputValueClass(Text.class);
            sortingJob.setOutputKeyClass(Text.class);
            sortingJob.setOutputValueClass(IntWritable.class);
            sortingJob.setInputFormatClass(KeyValueTextInputFormat.class);
            sortingJob.setSortComparatorClass(DescendingKey.class);
            KeyValueTextInputFormat.addInputPath(sortingJob, new Path(args[1]));
            FileOutputFormat.setOutputPath(sortingJob, new Path(args[2]));

            FileSystem fs = FileSystem.get(conf);
            fs.deleteOnExit(new Path(args[1]));

            System.exit(sortingJob.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
