# yhdi
a batch data extract-load and incremnt data sync tool


## start
### modfiy configuration file
[batch-odi.properties](https://github.com/jielee361/yhdi/blob/master/conf/batch-odi.properties)
```
#抽取表,格式：库名.表名1|库名.表名2 . 如果要插入到目标库，格式：库名.表名1-目标表名1|库名.表名2-目标表名2
tables=JY15XJ02900_RP_SJ.iy05
#抽取的数据文件存放目录
datafile.patch=D:\\data\\tmpdata
#分隔符
record.field=\t
#换行符
record.record=\n
#抽取模式。1：只抽取到文件；2：抽取并加载到目标库
extract.mode=1
#单表抽取并行度
extract.parallel=3
#jdbc fetchsize
extract.fetchsize=500
#线程池最大大小
thread.num=6
#多表并行抽取
table.isparallel=false
#打印条数间隔
record.print.size=10000
#源库类型
sdb.kind=oracle
sdb.url=jdbc:oracle:thin:@10.160.10.140:1521/pdbxnb
sdb.username=JY15XJ02900_RP_SJ
sdb.password=JY15XJ02900_RP_SJ
#目标库类型
tdb.kind=carbondata
tdb.url=jdbc:hive2://192.168.26.220:10016/carbon1

```

### start shell
```
sh ./start-batch-odi.sh
```
