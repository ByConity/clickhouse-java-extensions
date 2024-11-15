package org.byconity.iceberg.demo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class IceBergDemo {
    private final static Schema POSITIONAL_DELETE_SCHEMA =
            // Those field ids are reserved
            new Schema(Types.NestedField.required(2147483546, "file_path", Types.StringType.get()),
                    Types.NestedField.required(2147483545, "pos", Types.LongType.get()));

    private final FileSystem fileSystem;
    private final Path warehousePath;
    private final HadoopCatalog hadoopCatalog;
    private final List<Record> records = Lists.newArrayList();

    private Table table;
    private String dataFilePath;
    private Schema idEqdeleteSchema;

    public IceBergDemo(String host, int port) throws IOException {
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", String.format("hdfs://%s:%d", host, port));
        hdfsConf.set("dfs.client.use.datanode.hostname", "true");
        this.fileSystem = FileSystem.get(hdfsConf);
        // Use full path here, including the protocol, otherwise, spark cannot parse metadata
        // correctly
        this.warehousePath = new Path(String.format("hdfs://%s:%d/user/iceberg/demo", host, port));
        this.hadoopCatalog = new HadoopCatalog(hdfsConf, warehousePath.toString());
    }

    public void run() throws IOException {
        try {
            createTable();
            writeDataToTable();
            readDataFromTable();
            deleteIdEqualsTo(1);
            deleteSpecificRowByPosition(2);
            readDataFromTable();
        } finally {
            close();
        }
    }

    private void close() throws IOException {
        if (hadoopCatalog != null) {
            hadoopCatalog.close();
        }
    }

    private void clearPath() throws IOException {
        fileSystem.delete(warehousePath, true);
        fileSystem.mkdirs(warehousePath);
    }

    private void createTable() throws IOException {
        clearPath();


        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.IntegerType.get()));

        String namespaceName = "demo_namespace";
        Namespace namespace = Namespace.of(namespaceName);

        List<Namespace> namespaces = hadoopCatalog.listNamespaces();
        if (!namespaces.contains(namespace)) {
            hadoopCatalog.createNamespace(namespace);
        }

        String tablename = "demo_table";
        TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tablename);
        List<TableIdentifier> tableIdentifiers = hadoopCatalog.listTables(namespace);
        if (!tableIdentifiers.contains(tableIdentifier)) {
            hadoopCatalog.createTable(tableIdentifier, schema);
        }

        table = hadoopCatalog.loadTable(tableIdentifier);
    }

    private Record buildRecord(int id, String name, int age) {
        Record record = GenericRecord.create(table.schema());
        record.setField("id", id);
        record.setField("name", name);
        record.setField("age", age);
        records.add(record);
        return record;
    }

    private void writeDataToTable() throws IOException {
        try (FileIO io = table.io()) {
            dataFilePath = table.location() + String.format("/data/%s.parquet", UUID.randomUUID());
            OutputFile outputFile = io.newOutputFile(dataFilePath);

            try (FileAppender<Record> writer = Parquet.write(outputFile).schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter).build()) {
                writer.add(buildRecord(1, "Alice", 30));
                writer.add(buildRecord(2, "Tom", 18));
                writer.add(buildRecord(3, "Jerry", 22));
            }

            DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withInputFile(outputFile.toInputFile()).withRecordCount(1)
                    .withFormat(FileFormat.PARQUET).build();

            AppendFiles append = table.newAppend();
            append.appendFile(dataFile);
            append.commit();
        }
    }

    private void deleteSpecificRowByPosition(long position) throws IOException {
        try (FileIO io = table.io()) {
            OutputFile outputFile = io.newOutputFile(
                    table.location() + "/pos-deletes-" + UUID.randomUUID() + ".parquet");

            PositionDeleteWriter<Record> writer = Parquet.writeDeletes(outputFile).forTable(table)
                    .rowSchema(table.schema()).createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite().withSpec(PartitionSpec.unpartitioned()).buildPositionWriter();

            PositionDelete<Record> record = PositionDelete.create();
            record = record.set(dataFilePath, position, records.get((int) position));
            try (Closeable ignore = writer) {
                writer.write(record);
            }

            table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        }

        table.refresh();
    }

    private void deleteIdEqualsTo(int id) throws IOException {
        idEqdeleteSchema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

        try (FileIO io = table.io()) {
            OutputFile outputFile = io.newOutputFile(
                    table.location() + "/equality-deletes-" + UUID.randomUUID() + ".parquet");

            EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(outputFile).forTable(table)
                    .rowSchema(idEqdeleteSchema).createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite().equalityFieldIds(1).buildEqualityWriter();

            Record deleteRecord = GenericRecord.create(idEqdeleteSchema);
            deleteRecord.setField("id", id);
            try (Closeable ignore = writer) {
                writer.write(deleteRecord);
            }

            RowDelta rowDelta = table.newRowDelta();
            rowDelta.addDeletes(writer.toDeleteFile()); // Here, the writer must be at closed state
            rowDelta.commit();
        }

        table.refresh();
    }

    private void readDataFromTable() throws IOException {
        System.out.println("Current Snapshot ID: " + table.currentSnapshot().snapshotId());

        TableScan scan = table.newScan();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                List<DeleteFile> deletes = task.deletes();
                Set<Integer> deletedIds = Sets.newHashSet();
                Set<Long> deletedPos = Sets.newHashSet();
                for (DeleteFile delete : deletes) {
                    switch (delete.content()) {
                        case EQUALITY_DELETES:
                            try (FileIO io = table.io()) {
                                InputFile inputFile = io.newInputFile(delete.path().toString());
                                try (CloseableIterable<Record> records = Parquet.read(inputFile)
                                        .project(idEqdeleteSchema)
                                        .createReaderFunc(messageType -> GenericParquetReaders
                                                .buildReader(idEqdeleteSchema, messageType))
                                        .build()) {

                                    for (Record record : records) {
                                        System.out.println("Equality delete record: " + record);
                                        deletedIds.add((int) record.getField("id"));
                                    }
                                }
                            }
                            break;
                        case POSITION_DELETES:
                            try (FileIO io = table.io()) {
                                InputFile inputFile = io.newInputFile(delete.path().toString());
                                try (CloseableIterable<Record> records = Parquet.read(inputFile)
                                        .project(POSITIONAL_DELETE_SCHEMA)
                                        .createReaderFunc(messageType -> GenericParquetReaders
                                                .buildReader(POSITIONAL_DELETE_SCHEMA, messageType))
                                        .build()) {

                                    for (Record record : records) {
                                        System.out.println("Position delete record: " + record);
                                        deletedPos.add((long) record.getField("pos"));
                                    }
                                }
                            }
                            break;
                    }
                }
                try (FileIO io = table.io()) {
                    InputFile inputFile = io.newInputFile(task.file().path().toString());
                    try (CloseableIterable<Record> records =
                            Parquet.read(inputFile).project(table.schema())
                                    .createReaderFunc(messageType -> GenericParquetReaders
                                            .buildReader(table.schema(), messageType))
                                    .build()) {

                        long pos = -1;
                        for (Record record : records) {
                            pos++;
                            if (!deletedIds.contains((int) record.getField("id"))
                                    && !deletedPos.contains(pos)) {
                                System.out.println(record);
                            }
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        IceBergDemo iceBergDemo = new IceBergDemo("iceberg-hadoop", 8020);
        iceBergDemo.run();
    }
}
