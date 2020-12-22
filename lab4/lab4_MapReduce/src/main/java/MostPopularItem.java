
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MostPopularItem {
    public static boolean isNumeric(String str){  

        for (int i = 0; i < str.length(); i++){  
    
            if (!Character.isDigit(str.charAt(i))){  
    
                return false;  
    
             }  
    
        }  
    
         return true;  
    
    }  

    public static class SortMapper2
            extends Mapper<Object,Text,Text,LongWritable>{

        HashMap<String, Long> map = new HashMap<String, Long>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String fields[] = value.toString().split(",");

            if (fields.length != 9) {
                return;
            }
            if (!isNumeric(fields[8])){//去除掉空值或其他违规数据的情况
                return;
            }
            if (Integer.parseInt(fields[8])==0){//如果只是点击，则不计数
                return;
            }
            String item= fields[3];

            long count = map.getOrDefault(item, -1L);
            if (count==-1L)//判断该词是否已存在于hash表中
                map.put(item,1L);//不存在，加入新词
            else
                map.replace(item,count+1);//存在，词频加一
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            //将当前文件块内关键词的频度输出给reducer
            for (String keyWord : map.keySet()) {
                context.write(new Text(keyWord), new LongWritable(map.get(keyWord)));
            }
        }

    }

    public static class Pair<E extends Object, F extends Object> {
        private E first;
        private F second;
        public Pair(E key,F value){
            this.first=key;
            this.second=value;
        }
        public E getFirst() {
            return first;
        }
        public E getKey() {
            return first;
        }
        public F getValue() {
            return second;
        }
        public void setFirst(E first) {
            this.first = first;
        }
        public F getSecond() {
            return second;
        }
        public void setSecond(F second) {
            this.second = second;
        }
    }
    public static class SortReducer2
            extends Reducer<Text, LongWritable, Text,LongWritable>
    {
        public static int K = 100;//选出频次最大的K条关键词

        //小顶堆，容量K，用于快速删除词频最小的元素
        PriorityQueue<Pair<String, Long>> minHeap = new PriorityQueue<>((p1, p2) -> (int) (p1.getValue() - p2.getValue()));

        //每次传入的参数为key相同的values的集合
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable count : values) {
                //依次取出每个mapper统计的关键词key的频次，加起来
                total += count.get();
            }

            Pair<String, Long> tmp = new Pair<>(key.toString(), total);
            minHeap.add(tmp);//向小顶堆插入新的关键词词频
            if (minHeap.size() > K)//若小顶堆容量达到要求的上限
                minHeap.poll();//删除堆顶最小的元素，保持TopK
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Pair<String, Long>> list = new ArrayList<>();
            //从小顶堆中取出数据，便于排序
            for (Pair<String, Long> p : minHeap)
                list.add(p);

            //对搜索词频前K个元素排序
            Collections.sort(list, ((p1, p2) -> (int) (p2.getValue() - p1.getValue())));

            //reducer的输出，按搜索词频排好序的TopK关键词
            for (Pair<String, Long> t : list)
                context.write(new Text(t.getKey()), new LongWritable(t.getValue()));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String sourcePath=args[0];
        String outputPath=args[1];
        Job job = new Job(conf, "CountWords");
        job.setMapperClass(SortMapper2.class);
        job.setReducerClass(SortReducer2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(sourcePath));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);

    }
}