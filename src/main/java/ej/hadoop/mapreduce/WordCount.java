package ej.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    /**
     * Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     *   KEYIN - the key input type to the Mapper
     *   VALUEIN - the value input type to the Mapper
     *   KEYOUT - the key output type from the Mapper
     *   VALUEOUT - the value output type from the Mapper
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * The Mapper implementation, via the map method, processes one line at a time, as provided by the specified TextInputFormat.
         * It then splits the line into tokens separated by whitespaces, via the StringTokenizer, and emits a key-value pair of < <word>, 1>.
         *
         * For example, if given input files below
         *     $ bin/hadoop fs -cat input/file01
         *     Hello World Bye World
         *     $ bin/hadoop fs -cat input/file02
         *     Hello Hadoop Goodbye Hadoop
         *
         * the first map emits
         *     < Hello, 1>
         *     < World, 1>
         *     < Bye, 1>
         *     < World, 1>
         *
         * the second map emits
         *     < Hello, 1>
         *     < Hadoop, 1>
         *     < Goodbye, 1>
         *     < Hadoop, 1>
         *
         * @param key - KEYIN
         * @param value - VALUEIN
         * @param context - org.apache.hadoop.mapreduce.Mapper.Context, The context that is given to the Mapper.
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                /** Context.write(KEYOUT key, VALUEOUT value) - Generate an output key/value pair. */
                context.write(word, one);
            }
        }
    }

    /**
     * Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * The Reducer implementation, via the reduce method just sums up the values, which are the occurrence counts
         * for each key (i.e. words in this example).
         * The framework groups Reducer inputs by keys (since different mappers may have output the same key) in the
         * sort stage before reduce() is called.
         *
         * @param key - KEYIN
         * @param values - Iterable<VALUEIN>
         * @param context - org.apache.hadoop.mapreduce.Reducer.Context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);

        }
    }

    /**
     * The main method specifies various facets of the job, such as the input/output paths (passed via the command line),
     * key/value types, input/output formats etc.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);

        /**
         * WordCount also specifies a combiner. Hence, the output of each map is passed through the local combiner (which
         * is same as the Reducer as per the job configuration) for local aggregation, after being sorted on the keys.
         *
         * The output of the first map:
         *   < Bye, 1>
         *   < Hello, 1>
         *   < World, 2>
         *
         * The output of the second map:
         *   < Goodbye, 1>
         *   < Hadoop, 2>
         *   < Hello, 1>
         */
        job.setCombinerClass(IntSumReducer.class);

        /**
         * output of the Reduce job is
         *   < Bye, 1>
         *   < Goodbye, 1>
         *   < Hadoop, 2>
         *   < Hello, 2>
         *   < World, 2>
         */
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        /** calls the job.waitForCompletion to submit the job and monitor its progress. */
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
