# Readme

再次注意，在Windows下使用hdfs时貌似只能使用绝对路径来input

![](.\image\133053.jpg)

map和reduce不需要做过多操作，只需要保证输出到tempdir中的文件格式和后面第二次job处理时的格式匹配就好，这里为了统一，把输出和原来的文件保持一致

map和reduce分别见源代码中的MergeMapper类的MergeReducer类



## 使用自定义数据类型

:pushpin:  源代码见src/main/java/CF.java类

### 第一次job

第一次job把文件合并，方便后续操作，结果如下所示

![](.\image\111300.jpg)



其中map和reduce都不需要做过多处理，这需要保证其输出格式与第二次job相匹配，这里为了简单，保持其在源文件中的格式，仍然用源输入文件中的格式

### 自定义数据类型Pair

自定义数据类型一定要记得定义无参构造并且初始化变量，否则会报空指针错误

![](.\image\154729.jpg)

需要定义如下所示的无参构造函数：

```java
 public Pair() {
            this.a = new Text();
            this.b = new Text();
        }
```

最终定义的Pair数据类型如下：

```java
public static class Pair implements WritableComparable<Pair> {//修改key的定义，a，b各代表一个人
    private Text a;
    private Text b;

    public void set(Text s, Text t) {
        this.a = s;
        this.b = t;
    }

    public Pair() {
        this.a = new Text();
        this.b = new Text();
    }//必须定义这个无参构造函数

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

    public boolean equals(Object right) {
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
    }

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
```

:pen:  关于hashcode和equals两个函数：

- hashCode()方法和equal()方法的作用其实一样，在[Java](http://lib.csdn.net/base/java)里都是用来对比两个对象是否相等一致
- 重写的equal（）里一般比较的比较全面比较复杂，这样效率就比较低，而利用hashCode()进行对比，则只要生成一个hash值进行比较就可以了，效率很高
- hashCode()并不是完全可靠，有时候不同的对象他们生成的hashcode也会一样（生成hash值得公式可能存在的问题），所以hashCode()只能说是大部分时候可靠，并不是绝对可靠
- **所以解决方式是，每当需要对比的时候，首先用hashCode()去对比，如果hashCode()不一样，则表示这两个对象肯定不相等（也就是不必再用equal()去再对比了）,如果hashCode()相同，此时再对比他们的equal()，如果equal()也相同，则表示这两个对象是真的相同了**

在这个任务中即使不重新定义equals函数，也能得到正确结果，但是在数据变多之后，为了避免不同的key被当做同样的处理，最好还是定义一个equals函数！

### 第二次job

:pushpin:  map和reduce的代码见源代码中的CFMapper类和CFReducer类



##### 算法概述

1. map阶段：

   > 由于不存在单向好友，因此在一个人A的好友列表中，同时出现的两个人B、C就一定具有A这个共同好友。
   >
   > 例如：100, 200 300 400 500 中 (200,300)(200,400)(200,500)(300,400)(300,500)(400,500)就都一定具有100这个共同好友
   >
   > 因此，此时直接将一个pair如上述中的(200,300)作为key，将共同好友如上述中的100作为value即可

   代码如下：

   ```java
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
               StringTokenizer fitr = new StringTokenizer(next," ");//fitr中就是me的好友列表
               while(fitr.hasMoreElements()){
                   list.add(fitr.nextToken());
               }
               for( int k=0;k<list.size();k++){
                   for (int j=k+1;j<list.size();j++){
                       Text a=new Text(list.get(k).toString());
                       Text b=new Text(list.get(j).toString());
                       Pair p=new Pair(a,b);
                       context.write(p,me);
                   }//使用一个两层循环找出所有的pair，并且保证没有重复
               }
           }
       }
   }
   ```

2. reduce阶段

   > reduce阶段的任务就很简单了，不需要对数据做额外处理，就是把所有的共同好友合并成Text，按指定格式输出到文件中即可

:pen:   reduce时不能直接将定义的Pari输出，例如下面所示，无法显示：


![](.\image\155557.jpg)







最后在伪分布式下运行结果：

![](.\image\image-20201101105938868.jpg)



## 不使用自定义数据类型

:pushpin:  源代码见src/main/java/CFNP.java类

### 第一次job

因为不能使用自定义数据类型，所以我的做法是在第一次job中，在合并文件的同时，在reduce函数中把所有共同好友键值对输出，再在第二次job中做合并

即第一次job完成后的输出如下图所示



![](.\image\143029.jpg)

具体做法如下：

1. map函数

   > map函数和之前使用的map函数一致，不需要做多余工作，把一行 “100, 200 300 400 500” 中“100”作为key，“200 300 400 500”以一个完整的Text的形式作为value记录就可

2. reduce函数

   > reduce函数将传过来的value “200 300 400 500”处理一下，组合成没有重复的若干具有共同好友“100”的组合，作为一个完整的Text存入文件

   具体reduce代码如下：

   ```java
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
   ```

 

### 第二次job

第二次job不需要对数据做太多的复杂操作，MapReduce任务运行时，会自动将key相同的整合到一起，进行reduce操作

map：不需要做任何操作

```java
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
```

reduce:整理输出格式

```java
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
```



最终输出结果：

![](.\image\161949.jpg)

与第一种方法一致



两次job是迭代运行的，运行时设置好tempdir文件夹就可以了