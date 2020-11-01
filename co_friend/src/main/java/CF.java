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
        private Text b;

        public void set(Text s, Text t) {
            this.a = s;
            this.b = t;
        }

        public Pair() {
            this.a = new Text();
            this.b = new Text();
        }

        public Pair(Text a,Text b) {
            this.a = a;
            this.b = b;
        }

        public Text getA() {
            return a;
        }

        public Text getB() {
            return b;
        }

        public void write(DataOutput out) throws IOException {
            a.write(out);
            b.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            a.readFields(in);
            b.readFields(in);
        }

        /*public boolean equals(Object right) {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof Pair) {
                Pair r = (Pair) right;
                return r.getA()== a && r.getB() == b;
            } else {
                return false;
            }
        }*/

        public int compareTo(Pair p) {
            if(this.a.toString().equals(p.getA().toString())){
                return this.b.compareTo(p.getB());
        }
            else
                return this.a.compareTo(p.getA());
        }

        public int hashCode() {
            return a.hashCode()+b.hashCode()*147;
        }//必须把a，b都用上，保证相同的a，b的分到同一个reduce节点上
    }

    public class CFPartition extends Partitioner<Pair, NullWritable> {   // override the method
        @Override
        public int getPartition(Pair key, NullWritable value, int numReduceTasks) {
            return key.getB().hashCode()+614*key.getA().hashCode();
        }
    }


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
    public static class CFMapper
            extends Mapper<Object, Text, Pair, Text> {
        private Text me = new Text();
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line=value.toString() ;
            StringTokenizer itr = new StringTokenizer(line,",");
            ArrayList list = new ArrayList();
            while (itr.hasMoreTokens()) {
                String next = itr.nextToken();
                me.set(next);
                next=itr.nextToken();
                StringTokenizer fitr = new StringTokenizer(next," ");
                while(fitr.hasMoreElements()){
                    list.add(fitr.nextToken());
                }
                for( int k=0;k<list.size();k++){
                    for (int j=k+1;j<list.size();j++){
                        Text a=new Text(list.get(k).toString());
                        Text b=new Text(list.get(j).toString());
                        Pair p=new Pair(a,b);
                        context.write(p,me);
                    }
                }
            }
        }
    }

    public static class MergeReducer
            extends Reducer<Text, Text, Text, NullWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String sk= key.toString() + ",";
            for (Text val : values) {
                sk+=val.toString()+" ";
            }
            sk=sk.substring(0,sk.length()-1);
            context.write(new Text(sk),NullWritable.get());

        }
    }
    public static class CFReducer
            extends Reducer<Pair, Text, Text, Text> {
        private IntWritable result = new IntWritable();
        public void reduce(Pair key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String s=new String("[");
            ArrayList list = new ArrayList();
            for (Text val : values) {
                /*if (list.contains(val.toString())||val.equals(key.getA())||val.equals(key.getB())){
                    continue;
                }
                list.add(val.toString());*/
                s=s+val.toString()+",";
            }
            String outkey= "[";
            outkey+=key.getA().toString()+","+key.getB().toString()+"]";
            s=s.substring(0,s.length() - 1);
            s=s+"]";
            context.write(new Text(outkey),new Text(s));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path tempDir = new Path("CF-temp-output");
        Job job1 = Job.getInstance(conf, "file merge");
        job1.setMapperClass(MergeMapper.class);
        job1.setReducerClass(MergeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setJarByClass(CF.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job1, tempDir);
        job1.waitForCompletion(true);
        Job findCF = new Job(conf, "findCF");
        FileInputFormat.addInputPath(findCF, tempDir);
        findCF.setReducerClass(CFReducer.class);
        findCF.setMapperClass(CFMapper.class);
        findCF.setPartitionerClass(CFPartition.class);
        findCF.setOutputKeyClass(Pair.class);
        findCF.setOutputValueClass(Text.class);
        findCF.setJarByClass(CF.class);
        FileOutputFormat.setOutputPath(findCF, new Path(otherArgs.get(1)));
        findCF.waitForCompletion(true);
        System.exit(findCF.waitForCompletion(true) ? 0 : 1);
    }
}
