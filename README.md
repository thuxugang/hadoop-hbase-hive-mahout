hadoop-hbase
===

版本号
---

* hadoop 2.7.3
* hbase 1.2.3
* hive 2.0.0

监控端口
---

* yarn:192.168.1.26:8088
* hadoop:192.168.1.26:50070
* hbase:192.168.1.26:60010


注意事项：
---
* 本地hosts修改(加入bxf.hadoop1,bxf.hadoop2,bxf.hadoop3)
* hdfs上传文件权限修改(chmod 777)
* wifi连接boxfish

tips:
---

* 1.查看文件信息：hadoop fsck filename -files -blocks
* 2.查看集群信息：hadoop dfsadmin -report

Contributers:
---

* thuxugang
