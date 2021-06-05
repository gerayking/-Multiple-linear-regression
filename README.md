### QuickStart
#### 准备数据文件foo.csv
使用脚本data_pre.py生成数据文件，将数据文件放在resource目录下
#### 运行脚本
入口类为ShuffleDataJob
初始超参数配置可在PathUtil中进行配置  
foo.csv为[m,n]的数据样本，m为数据条目数量，n为n-1元变量，其中最后一个是y。
