# 如何构建

站内使用的HDFS是魔改版本，因此只能用定制版的Hadoop依赖来进行访问。但是这个定制版本的依赖，是无法直接访问标准的HDFS集群的。因此，针对不同的HDFS环境，需要使用不同的Hadoop依赖，这里，通过Maven的profile机制来解决这个问题，允许构建出2个版本的fat-jar

* 站内HDFS：默认的打包方式即可，例如`mvn clean package -DskipTests`
  * Profile `hdfs-byte`是默认激活的
* 开源HDFS：需要使用`mvn clean package -DskipTests -P hdfs-ce`，显示激活 Profile `hdfs-ce`

# Pmcli

```sh
alias pmcli='java -jar /path/to/paimon-reader-jar-with-dependencies.jar'
```

Options:

* `--help`：打印Help Doc
* `--catalog <arg>`：Json格式的内容；或者是一个Filepath，其内容也是一个Json。用于配置Catalog的连接参数
  * 此外，也可以通过配置环境变量`PAIMON_CATALOG`来实现
* `--action <action> [--arg <arg>]`：需要执行的动作，可选内容包括（部分参数需要通过`--arg`指定参数，参数可以是内容本身，或者包含内容的文件路径）：
  * `SHOW_DATABASES`：列出所有的Database，不需要`--arg`参数
  * `CREATE_DATABASE`：创建Database，`--arg`参数用于指定数据库名称
  * `DROP_DATABASE`：删除Database，`--arg`参数用于指定数据库名称
  * `SHOW_TABLES`：列出指定Database中的所有Table名称，`--arg`参数用于指定数据库名称
  * `CREATE_TABLE`：创建Table，`--arg`参数用于指定建表语句，格式为Json，配置示例可以参考 [resources](src%2Ftest%2Fresources)
  * `DROP_TABLE`：删除Table，`--arg`参数用于指定数据库表名称，例如`default.tbl_1`
  * `SHOW_TABLE_SCHEMA`：展示建表语句，`--arg`用于指定数据库表名称，例如`default.tbl_1`
  * `POPULATE_TABLE`：用于随机填充Table，`--arg`参数用于指定具体填充的参数，格式为Json
  * `LOAD_TABL`：用于导入数据，目前仅支持CSV格式的数据文件，`--arg`参数用于指定具体导入的参数，格式为Json。目前不支持导入复合类型的数据
  * `SHOW_DATA`：用于展示数据，`--arg`参数用于指定数据库表名称，例如`default.tbl_1`或`default.tbl_1.{col1,col2}`。目前不支持展示复合类型的数据，复合类型的数据显示的值为`<array>`、`<map>`、`<row>`等

## Catalog Config Demo

### LocalFileSystem

假设其路径为：`/tmp/paimon_catalog_local_config.conf`，后面示例中会用到

```json
{
    "metastoreType": "filesystem",
    "filesystemType": "LOCAL",
    "path": "/tmp/paimon_local_warehouse"
}
```

### HDFS

```json
{
    "metastoreType": "filesystem",
    "filesystemType": "HDFS",
    "warehouse": "hdfs://10.146.43.237:12000/user/paimon/warehouse"
}
```

### S3

```json
{
  "metastoreType": "filesystem",
  "filesystemType": "S3",
  "warehouse": "s3://<bucket>/<path>",
  "s3.endpoint.region": "cn-beijing",
  "s3.endpoint": "tos-s3-cn-beijing.volces.com",
  "s3.access-key": "xxx",
  "s3.secret-key": "xxxxx",
  "s3.path.style.access": false,
  "fs.s3a.connection.maximum": 1000
}
```

## SHOW_DATABASES

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action SHOW_DATABASES
```

## CREATE_DATABASE

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action CREATE_DATABASE --arg test_paimon
```

## DROP_DATABASE

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action DROP_DATABASE --arg test_paimon
```

## SHOW_TABLES

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action SHOW_TABLES --arg test_paimon
```

## CREATE_TABLE

Please refer to [CreateTableParams](src/main/java/org/byconity/paimon/cli/CreateTableProcessor.java) for all optional json fields.

```sh
cat > /tmp/paimon_create_table.json << 'EOF'
{
  "database": "test_paimon",
  "table": "tbl1",
  "columns": [
    {
      "name": "col_int",
      "type": "Int"
    },
    {
      "name": "col_varchar",
      "type": "nullable(Varchar(16))"
    }
  ],
  "partitionKeys": [
    "col_int"
  ]
}
EOF
```

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action CREATE_TABLE --arg /tmp/paimon_create_table.json
```

### File Format

可以指定添加`file.format`参数来指定文件格式，目前支持的格式包括：
* `orc`：默认
* `parquet`
* `avro`

Parquet的示例：

```json
{
  "database": "test_paimon",
  "table": "tbl1",
  "columns": [
    {
      "name": "col_int",
      "type": "Int"
    },
    {
      "name": "col_varchar",
      "type": "nullable(Varchar(16))"
    }
  ],
  "partitionKeys": [
    "col_int"
  ],
  "schemaOptions": {
    "file.format": "parquet",
    "file.compression": "zstd"
  }
}
```

## DROP_TABLE

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action DROP_TABLE --arg test_paimon.tbl1
```

## SHOW_TABLE_SCHEMA

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action SHOW_TABLE_SCHEMA --arg test_paimon.tbl1
```

## POPULATE_TABLE

Please refer to [PopulateTableParams](src/main/java/org/byconity/paimon/cli/PopulateTableProcessor.java) for all optional json fields.

```sh
cat > /tmp/paimon_populate_table.json << 'EOF'
{
  "database": "test_paimon",
  "table": "tbl1",
  "rows": 16,
  "nullableRate": 0.1
}
EOF
```

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action POPULATE_TABLE --arg /tmp/paimon_populate_table.json
```

### FAQ1: empty fields are illegal, the field should be ommited completely instead

Currently, parquet format has not been fully supported. This error may occur when populate table with complex type including `array`, `map`, `row`.

## LOAD_TABL

Please refer to [LoadTableParams](src/main/java/org/byconity/paimon/cli/LoadTableProcessor.java) for all optional json fields.

```sh
cat > /tmp/tbl1.csv << 'EOF'
1,"Hello"
2,"World"
3,
EOF
```

```sh
cat > /tmp/paimon_load_table.json << 'EOF'
{
  "database": "test_paimon",
  "table": "tbl1",
  "filePath": "/tmp/tbl1.csv"
}
EOF
```

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action LOAD_TABLE --arg /tmp/paimon_load_table.json
```

## SHOW_DATA

```sh
export PAIMON_CATALOG=/tmp/paimon_catalog_local_config.conf

pmcli --action SHOW_DATA  --arg test_paimon.tbl1

pmcli --action SHOW_DATA  --arg "test_paimon.tbl1.{col1,col2}"
```
