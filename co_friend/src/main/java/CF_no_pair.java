
import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class CF_no_pair {
    public static class MergeMapper
            extends Mapper<Object, Text, Text, Text> {

        static enum CountersEnum {INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private Text me = new Text();
        private Text friends = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line=value.toString() ;
            StringTokenizer itr = new StringTokenizer(line,",");
            while (itr.hasMoreTokens()) {
                String next = itr.nextToken();
                me.set(next);
                next = itr.nextToken();
                friends.set(next);
                context.write(me, friends);
                Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);

            }
        }
    }
    public static class MergeReducer
            extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {
                ArrayList list = new ArrayList();
                String line = val.toString();
                StringTokenizer itr = new StringTokenizer(line, " ");
                while (itr.hasMoreTokens()) {
                    list.add(itr.nextToken());
                }
                for( int k=0;k<list.size();k++){
                    for (int j=k+1;j<list.size();j++){
                        Text a=new Text(list.get(k).toString());
                        Text b=new Text(list.get(j).toString());
                        Text keyout=new Text(a.toString()+","+b.toString()+" "+key.toString());
                        context.write(keyout,NullWritable.get());
                    }
                }
            }
        }
    }
    public static class CFMapper_no_pair
            extends Mapper<Object,Text,Text,Text>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line=value.toString() ;
            StringTokenizer itr = new StringTokenizer(line," ");
            while (itr.hasMoreTokens()) {
                String keyout = itr.nextToken();
                String valueout= itr.nextToken();
                context.write(new Text(keyout),new Text(valueout));
            }
        }
    }
    public static class CFReducer_no_pair
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String keyout= "[";
            String valueout= "[";
            keyout+=key.toString()+"]";
            for (Text val : values) {
               valueout+=val.toString()+",";

            }
            valueout=valueout.substring(0,valueout.length()-1)+"]";
            context.write(new Text(keyout),new Text(valueout));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path tempDir = new Path("CFNP-temp-output");
        Job job1 = Job.getInstance(conf, "file merge");
        job1.setMapperClass(MergeMapper.class);
        job1.setReducerClass(MergeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job1, tempDir);
        job1.waitForCompletion(true);
        Job findCF = new Job(conf, "findCF");
        FileInputFormat.addInputPath(findCF, tempDir);
        findCF.setReducerClass(CFReducer_no_pair.class);
        findCF.setMapperClass(CFMapper_no_pair.class);
        findCF.setOutputKeyClass(Text.class);
        findCF.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(findCF, new Path(otherArgs.get(1)));
        findCF.waitForCompletion(true);
        System.exit(findCF.waitForCompletion(true) ? 0 : 1);
    }
}

