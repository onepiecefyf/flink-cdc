
### 什么是Flink CDC
CDC 是 Change Data Capture（变更数据获取）的简称。核心思想是，监测并捕获数据库
的变动（包括数据或数据表的插入、更新以及删除等），将这些变更按发生的顺序完整记录
下来，写入到消息中间件中以供其他服务进行订阅及消费。



### 问题

#### 问题1
从 8.0.26 开始，MySQL 驱动程序更改了以下变量访问语义
```java
com.mysql.cj.CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME
```
此变量从公共改为私有

#### 解决方案
将mysql-connector-java设置为8.0.26以下版本