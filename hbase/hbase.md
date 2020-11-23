

参考教程：[Apache HBase ™ Reference Guide](http://hbase.apache.org/book.html#quickstart)

操作系统：Linux Ubuntu

# 单机模式



安装hbase，修改hbase-env.sh的内容，主要是自己的Java路径

启动hbase。成功：

![image-20201121165923457](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201121165923457.png)



# 伪分布式

![image-20201121185245464](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201121185245464.png)

关闭虚拟机再重启hdfs时发现namenode无法正常启动，打开log发现：

![image-20201123104711094](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123104711094.png)

解决方法：将core-site.xml里面设置tmp-dir为其他文件夹

![image-20201123105001211](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201123105001211.png)

# windows

考虑到编程的方便性，这里选择在Windows上再安装一遍，便于使用idea做maven的项目管理

:pencil:	windows下最好还是安装1.x版本，安装2.x版本时会出现一些问题

:pencil:	安装和配置都和linux下没有本质区别，只是脚本文件是cmd不是sh

jps显示Hmaster正常

![image-20201122102255266](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201122102255266.png)

可以看到hdfs下的hbase目录

![image-20201122102441293](C:\Users\wangs\AppData\Roaming\Typora\typora-user-images\image-20201122102441293.png)