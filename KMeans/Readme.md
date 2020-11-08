# KMeans方法实践

:pencil:  	output结果在 ./src/output文件夹中，命名方式为i_jKMoutput其中i表示分类的类别数目，j表示迭代的次数

:pencil:		代码逻辑：

	>- 示例代码将job分为两类，clusterCenterJob对应的是训练的过程，KMeansClusterJob对应的是预测的过程
	>- 在clusterCenterJob运行指定迭代次数前，先运行一个generateInitialCluster()函数，来获得初始的中心点，采用的是随机生成的方式
	>- clusterCenterJob迭代运行时，需要输出的只有不断更新的中心点信息，即输出文件中的cluster-k文件夹，其中k代表迭代的次数
	>- 定义了一个自定义value类型Cluster，实现了writable接口。cluster中包含clusterID、numOfPoints，以及一个为自定义数据类型Instance的center
	>
	>