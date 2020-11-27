

参考教程：[Apache HBase ™ Reference Guide](http://hbase.apache.org/book.html#quickstart)

操作系统：Linux Ubuntu、Windows 10

:warning:	md文档的图片没有上传，请查看pdf文档

# 单机模式



安装hbase，修改hbase-env.sh的内容，主要是自己的Java路径

启动hbase。成功：

![image-20201121165923457](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201121165923457.png)



# 伪分布式

更改hbase-site里的配置，完成伪分布式

![image-20201121185245464](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201121185245464.png)

:pencil:	**关闭虚拟机再重启hdfs时发现namenode无法正常启动，打开log发现：**

![image-20201123104711094](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123104711094.png)

原因可能是因为哪一次关闭出现了问题，导致tmp文件夹下某个文件在关机的时候被删除，但是hadoop没有得知这个信息（我估计就是stop-all引发的问题）

解决方法：将core-site.xml里面设置tmp-dir为其他文件夹，并且重新初始化

![image-20201123105001211](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123105001211.png)

:pencil:	**然后就会出现Hmaster消失的问题：**

![image-20201125170419282](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201125170419282.png)

换用外部zookeeper之后：

![image-20201125181920792](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201125181920792.png)



:pencil:	在确保了一切配置都正确之后，过一段时间，**Hmaster仍然消失，查一下log发现报错和之前一样**

![image-20201125182525033](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201125182525033.png)

都是 Failed to get of mater address

尝试了各种各样的方法都没法成功之后，我选择在另一个虚拟机上重装hadoop和hbase，**我觉得应该是版本不匹配的问题**

在网上找到了一个版本匹配情况：
![img](https://img-blog.csdnimg.cn/20200409145350325.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQxMDMyNDc0,size_16,color_FFFFFF,t_70)

按照这个表，分别安装了hadoop2.7.2和hbase1.4.13，配置好之后终于成功了	:sob:

![image-20201125200449099](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201125200449099.png)

![image-20201125200434184](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201125200434184.png)

# 

# windows

考虑到编程的方便性，这里选择在Windows上再安装一遍，便于使用idea做maven的项目管理

:pencil:	windows下最好还是安装1.x版本，安装2.x版本时会出现一些问题

:pencil:	安装和配置都和linux下没有本质区别，只是脚本文件是cmd不是sh

jps显示Hmaster正常

![image-20201122102255266](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201122102255266.png)

可以看到hdfs下的hbase目录

![image-20201122102441293](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201122102441293.png)



# Java代码实现

IDE：Idea 

OS：Windows 10

代码管理：Maven

代码见github仓库 https://github.com/wangskyGit/FBDPcode.git

pom.xml中需要增加的依赖如下,版本号与自己安装的hbase一致：

```xml
<dependencies>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>1.6.0</version>
        </dependency>
    </dependencies>
```

运行结果：

![image-20201123163006066](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123163006066.png)

运行成功！

# shell 运行

1. 创建表格，并插入数据

![image-20201123170907840](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123170907840.png)

2. scan students

   ![image-20201123171711651](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123171711651.png)

3. 查询学生来自的省

   <img src="C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123172345679.png" alt="image-20201123172345679" style="zoom: 80%;" />

4. （4）增加新的列Courses:English，并添加数据；

   ![image-20201123173739335](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123173739335.png)

5. 增加新的列族Contact和新列 Contact:Email，并添加数据；

   ![image-20201123173602827](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123173602827.png)



6. 删除students表

![image-20201123173951632](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123173951632.png)