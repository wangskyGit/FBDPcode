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


public class CF {

    public static class Pair implements WritableComparable<Pair> {//修改key的定义，a代表字符，b代表其出现的次数
        private Text a;
        private int b;
        public void set(Text s, int t){
            this.a=s;
            this.b=t;
        }
        public Pair(){

        }
        public Pair(Text a, int b){
            this.a = a;
            this.b = b;
        }
        public Text getA() {
            return a;
        }
        public int getB() {
            return b;
        }
        public void write(DataOutput out) throws IOException{
            a.write(out);
            out.writeInt(b);
        }
        public void readFields(DataInput in) throws IOException{
            a.readFields(in); b = in.readInt();
        }
        public int compareTo(Pair p){
            if(this.b<p.getB())return -1;
            else if(this.b>p.getB()) return -1;
            else {
                return this.a.compareTo(p.getA());
            }
        }
        public int hashCode()
        {
            return a.hashCode();
        }
    }

    public class SortPartition extends Partitioner<Pair,NullWritable> {   // override the method
        @Override
        public int getPartition(Pair key, NullWritable value, int numReduceTasks)
        {
            String term = key.getA().toString().split(",")[0];//<term,times>=>term
            return term.hashCode();
        }
    }
    public static class GroupingComparator extends WritableComparator//分组函数，只要pair里面的单词相同就在同一组
    {
        protected GroupingComparator()
        {
            super(Pair.class, true);
        }
        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            Pair ip1 = (Pair) w1;
            Pair ip2 = (Pair) w2;
            String l = ip1.getA().toString();
            String r = ip2.getA().toString();
            return l.compareTo(r);
        }
    }
    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class SortReducer2
            extends Reducer<IntWritable, Text, IntWritable, Text>
    {
        int t = 0;
        private Text left=new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException
        {
            //将排名前100的单词输出
            for (Text val : values) {
                left.set(val.toString()+","+key.toString());
                if (t<100){
                    t++;
                    context.write(new IntWritable(t), left);
                }
            }

        }
    }
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        static enum CountersEnum {INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", true)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            line = line.replaceAll("[\\pP\\p{Punct}]", "");

            StringTokenizer itr = new StringTokenizer(line);

            while (itr.hasMoreTokens()) {
                String next=itr.nextToken();
                word.set(next);
                if(patternsToSkip.contains(next))continue;//去除停词
                if (word.toString().length()<3)continue;//去除数量小于3的单词
                context.write(word, one);
                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /*
     * GenericOptionsParser：解释hadoop命令中[genericOptions]部分的一个类
     * hadoop命令的一般格式：bin/hadoop command [genericOptions] [commandOptions]
     * */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path tempDir = new Path("wordcount-temp-output");
        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);  //新增属性“wordcount.skip.patterns”,设置为true
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));

        FileOutputFormat.setOutputPath(job, tempDir);
        job.waitForCompletion(true);
        Job sort=new Job(conf,"sort");
        FileInputFormat.addInputPath(sort, tempDir);
        sort.setJarByClass(WordCount2.class);
        sort.setMapperClass(SortMapper2.class);
        sort.setReducerClass(SortReducer2.class);
        sort.setNumReduceTasks(1);
        sort.setOutputKeyClass(IntWritable.class);
        sort.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(sort,new Path(otherArgs.get(1)));
        sort.setSortComparatorClass(IntWritableDecreasingComparator.class);

        sort.waitForCompletion(true);
        System.exit(sort.waitForCompletion(true) ? 0 : 1);
    }
}
