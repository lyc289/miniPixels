## 成员变量含义:
#### <target_type>* vector: 用于存放目标类型数据的数组
#### writeIndex:目前填充到vector第几个元素
#### memoryUsage:vector内存占用量,增量为length*sizeof(target_type)
#### isNull:此位置是否有元素，add时将新填充的位置设为false
#### ensureSize编写方法:
1.调用父类函数</p>
2.判断lengh < size </p>
3.记录就数组地址
4.利用posix_memalign为vector分配新空间</p>
5。memcopy </p>
6.delete 旧数组</p>
7.增加memoryUsage</p>
8.调用resize</p>
## 调试：
```
LOAD -o /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/testdate -t /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/dateoutput -s struct<a:int,b:int> -n 10 -r \|
```
可以看到int类型数据的读写</p>
```
LOAD -o /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/testdate -t /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/dateoutput -s struct<a:date> -n 10 -r \|
```
查看date类型数据的读写
运行
```
/media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/build/debug/examples/pixels-example/pixels-example
```
并输入某一个output文件的路径看到duckdb的读取结果
decimal类型数据的读写
```
LOAD -o /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/test_decimal -t /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/decimaloutput/ -s struct<a:decimal(15,2)> -n 100 -r \|
```
timstamp数据的读写
```
LOAD -o /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/testtimestamp -t /media/renyongning/新加卷/learn/database_develop/lab2/miniPixels-Project/data/timestampoutput/ -s struct<a:timestamp> -n 100 -r \|
```
## 注意
1.记的更改ColumnWriterBuilder.cpp中的内容</p>
2.貌似int/long都用的long存储，encoder中关于int的写有些问题，是不是要改为和Long一样的样子？</p>
3.在duckdb度的代码中没有用到runlengthEncoding，因此write中应该不进入使用runlengthEncoding的分支语句</p>
4.timestamp的write需要用writeLongLE，buffersize应该是元素数量*sizeof(long)</p>
5.timestamp默认用10-6 s为精度</p>
6.所有的add接口都是接受的string，因此只要把string的编码解析正确即可</p>
