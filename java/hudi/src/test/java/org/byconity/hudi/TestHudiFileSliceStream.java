package org.byconity.hudi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.byconity.proto.HudiMeta;
import org.byconity.hudi.reader.HudiArrowReaderBuilder;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestHudiFileSliceStream {
    final static BufferAllocator allocator = new RootAllocator();

    byte[] serialize(Map<String, String> map) {
        HudiMeta.Properties.Builder builder = HudiMeta.Properties.newBuilder();
        map.forEach((k, v) -> {
            HudiMeta.Properties.KeyValue kv =
                    HudiMeta.Properties.KeyValue.newBuilder().setKey(k).setValue(v).build();
            builder.addProperties(kv);
        });
        return builder.build().toByteArray();
    }

    void runScanOnParams(byte[] params) throws IOException {
        HudiArrowReaderBuilder builder = HudiArrowReaderBuilder.create(params);
        System.out.println(builder);

        ArrowReader stream = builder.build();
        while (stream.loadNextBatch()) {
            System.out.println(stream.getVectorSchemaRoot().contentToTSVString());
        }
        stream.close();
    }

    /*
     * CREATE TABLE `test_hudi_mor` ( `uuid` STRING, `ts` int, `a` int, `b` string, `c` array<int>,
     * `d` map<string, int>, `e` struct<a:int, b:string>) USING hudi TBLPROPERTIES ( 'primaryKey' =
     * 'uuid', 'preCombineField' = 'ts', 'type' = 'mor');
     * 
     * spark-sql> select a,b,c,d,e from test_hudi_mor; a b c d e 1 hello [10,20,30]
     * {"key1":1,"key2":2} {"a":10,"b":"world"}
     */

    @Test
    public void scanTestOnPrimitiveType() throws IOException {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiFileSliceStream.class.getResource("/test_hudi_mor");
        String basePath = resource.getPath().toString();
        params.put("fetch_size", "4096");
        params.put("base_path", basePath);
        params.put("data_file_path", basePath
                + "/64798197-be6a-4eca-9898-0c2ed75b9d65-0_0-54-41_20230105142938081.parquet");
        params.put("delta_file_paths", basePath
                + "/.64798197-be6a-4eca-9898-0c2ed75b9d65-0_20230105142938081.log.1_0-95-78");
        params.put("hive_column_names",
                "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,uuid,ts,a,b,c,d,e");
        params.put("hive_column_types",
                "string#string#string#string#string#string#int#int#string#array<int>#map<string,int>#struct<a:int,b:string>");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "436081");
        params.put("input_format",
                "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "a,b,c,d,e");
        runScanOnParams(serialize(params));
    }

    /*
     * CREATE TABLE `parquet_all_types`( `t_null_string` string, `t_null_varchar` varchar(65535),
     * `t_null_char` char(10), `t_null_decimal_precision_2` decimal(2,1),
     * `t_null_decimal_precision_4` decimal(4,2), `t_null_decimal_precision_8` decimal(8,4),
     * `t_null_decimal_precision_17` decimal(17,8), `t_null_decimal_precision_18` decimal(18,8),
     * `t_null_decimal_precision_38` decimal(38,16), `t_empty_string` string, `t_string` string,
     * `t_empty_varchar` varchar(65535), `t_varchar` varchar(65535), `t_varchar_max_length`
     * varchar(65535), `t_char` char(10), `t_int` int, `t_bigint` bigint, `t_float` float,
     * `t_double` double, `t_boolean_true` boolean, `t_boolean_false` boolean,
     * `t_decimal_precision_2` decimal(2,1), `t_decimal_precision_4` decimal(4,2),
     * `t_decimal_precision_8` decimal(8,4), `t_decimal_precision_17` decimal(17,8),
     * `t_decimal_precision_18` decimal(18,8), `t_decimal_precision_38` decimal(38,16), `t_binary`
     * binary, `t_map_string` map<string,string>, `t_map_varchar`
     * map<varchar(65535),varchar(65535)>, `t_map_char` map<char(10),char(10)>, `t_map_int`
     * map<int,int>, `t_map_bigint` map<bigint,bigint>, `t_map_float` map<float,float>,
     * `t_map_double` map<double,double>, `t_map_boolean` map<boolean,boolean>,
     * `t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>, `t_map_decimal_precision_4`
     * map<decimal(4,2),decimal(4,2)>, `t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
     * `t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>, `t_map_decimal_precision_18`
     * map<decimal(18,8),decimal(18,8)>, `t_map_decimal_precision_38`
     * map<decimal(38,16),decimal(38,16)>, `t_array_string` array<string>, `t_array_int` array<int>,
     * `t_array_bigint` array<bigint>, `t_array_float` array<float>, `t_array_double` array<double>,
     * `t_array_boolean` array<boolean>, `t_array_varchar` array<varchar(65535)>, `t_array_char`
     * array<char(10)>, `t_array_decimal_precision_2` array<decimal(2,1)>,
     * `t_array_decimal_precision_4` array<decimal(4,2)>, `t_array_decimal_precision_8`
     * array<decimal(8,4)>, `t_array_decimal_precision_17` array<decimal(17,8)>,
     * `t_array_decimal_precision_18` array<decimal(18,8)>, `t_array_decimal_precision_38`
     * array<decimal(38,16)>, `t_struct_bigint` struct<s_bigint:bigint>, `t_complex`
     * map<string,array<struct<s_int:int>>>, `t_struct_nested` struct<struct_field:array<string>>,
     * `t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
     * `t_struct_non_nulls_after_nulls`
     * struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
     * `t_nested_struct_non_nulls_after_nulls`
     * struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,
     * nested_struct_field2:string>>, `t_map_null_value` map<string,string>,
     * `t_array_string_starting_with_nulls` array<string>, `t_array_string_with_nulls_in_between`
     * array<string>, `t_array_string_ending_with_nulls` array<string>, `t_array_string_all_nulls`
     * array<string> ) ROW FORMAT SERDE
     * 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION
     * '/user/doris/preinstalled_data/parquet_table/parquet_all_types' TBLPROPERTIES (
     * 'transient_lastDdlTime'='1681213018');
     */
    @Test
    public void scanTestOnAllTypes() throws IOException {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiFileSliceStream.class.getResource("/hive_all_types");
        String basePath = resource.getPath().toString();
        params.put("fetch_size", "512");
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/parquet_all_types");
        params.put("hive_column_names",
                "t_null_string,t_null_varchar,t_null_char,t_null_decimal_precision_2,t_null_decimal_precision_4,t_null_decimal_precision_8,t_null_decimal_precision_17,t_null_decimal_precision_18,t_null_decimal_precision_38,t_empty_string,t_string,t_empty_varchar,t_varchar,t_varchar_max_length,t_char,t_int,t_bigint,t_float,t_double,t_boolean_true,t_boolean_false,t_decimal_precision_2,t_decimal_precision_4,t_decimal_precision_8,t_decimal_precision_17,t_decimal_precision_18,t_decimal_precision_38,t_binary,t_map_string,t_map_varchar,t_map_char,t_map_int,t_map_bigint,t_map_float,t_map_double,t_map_boolean,t_map_decimal_precision_2,t_map_decimal_precision_4,t_map_decimal_precision_8,t_map_decimal_precision_17,t_map_decimal_precision_18,t_map_decimal_precision_38,t_array_string,t_array_int,t_array_bigint,t_array_float,t_array_double,t_array_boolean,t_array_varchar,t_array_char,t_array_decimal_precision_2,t_array_decimal_precision_4,t_array_decimal_precision_8,t_array_decimal_precision_17,t_array_decimal_precision_18,t_array_decimal_precision_38,t_struct_bigint,t_complex,t_struct_nested,t_struct_null,t_struct_non_nulls_after_nulls,t_nested_struct_non_nulls_after_nulls,t_map_null_value,t_array_string_starting_with_nulls,t_array_string_with_nulls_in_between,t_array_string_ending_with_nulls,t_array_string_all_nulls");
        params.put("hive_column_types",
                "string#varchar(65535)#char(10)#decimal(2,1)#decimal(4,2)#decimal(8,4)#decimal(17,8)#decimal(18,8)#decimal(38,16)#string#string#varchar(65535)#varchar(65535)#varchar(65535)#char(10)#int#bigint#float#double#boolean#boolean#decimal(2,1)#decimal(4,2)#decimal(8,4)#decimal(17,8)#decimal(18,8)#decimal(38,16)#binary#map<string,string>#map<varchar(65535),varchar(65535)>#map<char(10),char(10)>#map<int,int>#map<bigint,bigint>#map<float,float>#map<double,double>#map<boolean,boolean>#map<decimal(2,1),decimal(2,1)>#map<decimal(4,2),decimal(4,2)>#map<decimal(8,4),decimal(8,4)>#map<decimal(17,8),decimal(17,8)>#map<decimal(18,8),decimal(18,8)>#map<decimal(38,16),decimal(38,16)>#array<string>#array<int>#array<bigint>#array<float>#array<double>#array<boolean>#array<varchar(65535)>#array<char(10)>#array<decimal(2,1)>#array<decimal(4,2)>#array<decimal(8,4)>#array<decimal(17,8)>#array<decimal(18,8)>#array<decimal(38,16)>#struct<s_bigint:bigint>#map<string,array<struct<s_int:int>>>#struct<struct_field:array<string>>#struct<struct_field_null:string,struct_field_null2:string>#struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>#struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>#map<string,string>#array<string>#array<string>#array<string>#array<string>");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "89038");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields",
                "t_null_string,t_null_varchar,t_null_char,t_null_decimal_precision_2,t_null_decimal_precision_4,t_null_decimal_precision_8,t_null_decimal_precision_17,t_null_decimal_precision_18,t_null_decimal_precision_38,t_empty_string,t_string,t_empty_varchar,t_varchar,t_varchar_max_length,t_char,t_int,t_bigint,t_float,t_double,t_boolean_true,t_boolean_false,t_decimal_precision_2,t_decimal_precision_4,t_decimal_precision_8,t_decimal_precision_17,t_decimal_precision_18,t_decimal_precision_38,t_binary,t_map_string,t_map_varchar,t_map_char,t_map_int,t_map_bigint,t_map_float,t_map_double,t_map_boolean,t_map_decimal_precision_2,t_map_decimal_precision_4,t_map_decimal_precision_8,t_map_decimal_precision_17,t_map_decimal_precision_18,t_map_decimal_precision_38,t_array_string,t_array_int,t_array_bigint,t_array_float,t_array_double,t_array_boolean,t_array_varchar,t_array_char,t_array_decimal_precision_2,t_array_decimal_precision_4,t_array_decimal_precision_8,t_array_decimal_precision_17,t_array_decimal_precision_18,t_array_decimal_precision_38,t_struct_bigint,t_complex,t_struct_nested,t_struct_null,t_struct_non_nulls_after_nulls,t_nested_struct_non_nulls_after_nulls,t_map_null_value,t_array_string_starting_with_nulls,t_array_string_with_nulls_in_between,t_array_string_ending_with_nulls,t_array_string_all_nulls");
        runScanOnParams(serialize(params));
    }

    /*
     * CREATE TABLE `parquet_timestamp_millis`( test timestamp ) ROW FORMAT SERDE
     * 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION
     * '/user/doris/preinstalled_data/parquet_table/parquet_timestamp_millis';
     */
    @Test
    public void scanTimestampMillisType() throws IOException {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiFileSliceStream.class.getResource("/hive_all_types");
        String basePath = resource.getPath().toString();
        params.put("fetch_size", "512");
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/parquet_timestamp_millis");
        params.put("hive_column_names", "test");
        params.put("hive_column_types", "timestamp");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "89038");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "test");
        runScanOnParams(serialize(params));
    }

    @Test
    public void scanTimestampMicrosType() throws IOException {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiFileSliceStream.class.getResource("/hive_all_types");
        String basePath = resource.getPath().toString();
        params.put("fetch_size", "512");
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/parquet_timestamp_micros");
        params.put("hive_column_names", "test");
        params.put("hive_column_types", "timestamp");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "89038");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "test");
        runScanOnParams(serialize(params));
    }

    @Test
    public void scanTimestampNanosType() throws IOException {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiFileSliceStream.class.getResource("/hive_all_types");
        String basePath = resource.getPath().toString();
        params.put("fetch_size", "512");
        params.put("base_path", basePath);
        params.put("data_file_path", basePath + "/parquet_timestamp_nanos");
        params.put("hive_column_names", "test");
        params.put("hive_column_types", "timestamp");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "89038");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "test");
        runScanOnParams(serialize(params));
    }
}
