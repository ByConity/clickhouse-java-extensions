package org.byconity.iceberg.cli;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.Types;
import org.byconity.iceberg.util.UUIDUtils;
import org.byconity.iceberg.writer.NativePartitionedWriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public class PopulateTableProcessor extends ActionProcessor {
    @SuppressWarnings("all")
    private static final class PopulateTableParams {
        @SerializedName("database")
        private String database;
        @SerializedName("table")
        private String table;
        @SerializedName("rows")
        private int rows = 16;
        @SerializedName("nullableRate")
        private double nullableRate = 0.1;
    }


    private final PopulateTableParams params;

    public static void process(Catalog catalog, String arg) throws Exception {
        PopulateTableParams params;
        try {
            params = GSON.fromJson(arg, PopulateTableParams.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Wrong arg format for action '%s', it should be json.",
                            ActionType.POPULATE_TABLE.name()));
        }
        new PopulateTableProcessor(catalog, params).doProcess();
    }

    private PopulateTableProcessor(Catalog catalog, PopulateTableParams params) {
        super(catalog);
        this.params = params;
    }

    @Override
    protected void doProcess() throws Exception {
        TableIdentifier identifier = TableIdentifier.of(params.database, params.table);
        if (!catalog.tableExists(identifier)) {
            System.out.printf("Table '%s.%s' does not exist.\n", params.database, params.table);
            return;
        }
        Table icebergTable = catalog.loadTable(identifier);
        Schema schema = icebergTable.schema();
        List<Types.NestedField> fields = schema.columns();

        List<RandomGenerator> generators = Lists.newArrayList();
        for (Types.NestedField field : fields) {
            generators.add(RandomGenerator.build(params, field));
        }

        FileFormat fileFormat = FileFormat.fromString(icebergTable.properties()
                .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()));

        PartitionSpec spec = icebergTable.spec();
        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(schema, spec);
        // partitionId and taskId and operationId will be used as the file name, we only need to
        // guarantee the combination is unique.
        OutputFileFactory fileFactory =
                OutputFileFactory.builderFor(icebergTable, 1, System.currentTimeMillis())
                        .format(fileFormat).operationId(UUID.randomUUID().toString()).build();
        long targetFileSize = 512 * 1024 * 1024;
        BaseTaskWriter<Record> writer;

        if (spec.isUnpartitioned()) {
            writer = new UnpartitionedWriter<>(spec, fileFormat, appenderFactory, fileFactory,
                    icebergTable.io(), targetFileSize);
        } else {
            writer = new NativePartitionedWriter(spec, fileFormat, appenderFactory, fileFactory,
                    icebergTable.io(), targetFileSize, schema);
        }

        for (int i = 0; i < params.rows; i++) {
            GenericRecord record = GenericRecord.create(schema);
            int pos = 0;
            for (RandomGenerator generator : generators) {
                record.set(pos++, generator.next());
            }
            writer.write(record);
        }
        writer.close();

        AppendFiles append = icebergTable.newAppend();
        for (DataFile dataFile : writer.dataFiles()) {
            append.appendFile(dataFile);
        }
        append.commit();

        System.out.printf("Populate table '%s.%s' successfully, %d rows have been generated.\n",
                params.database, params.table, params.rows);
    }


    private static abstract class RandomGenerator {
        protected static final Random RANDOM = new Random();
        protected final PopulateTableParams params;
        protected final Types.NestedField field;

        static RandomGenerator build(PopulateTableParams params, Types.NestedField field) {
            switch (field.type().typeId()) {
                case BOOLEAN:
                    return new BooleanRandomGenerator(params, field);
                case INTEGER:
                    return new IntRandomGenerator(params, field);
                case LONG:
                    return new BigintRandomGenerator(params, field);
                case FLOAT:
                    return new FloatRandomGenerator(params, field);
                case DOUBLE:
                    return new DoubleRandomGenerator(params, field);
                case DATE:
                    return new DateRandomGenerator(params, field);
                case TIMESTAMP:
                    return new TimestampRandomGenerator(params, field);
                case UUID:
                    return new UUIDRandomGenerator(params, field);
                case STRING:
                    return new StringRandomGenerator(params, field);
                case FIXED:
                    return new FixedRandomGenerator(params, field);
                case BINARY:
                    return new BinaryRandomGenerator(params, field);
                case DECIMAL:
                    return new DecimalRandomGenerator(params, field);
                case LIST:
                    return new ListRandomGenerator(params, field);
                case MAP:
                    return new MapRandomGenerator(params, field);
                case STRUCT:
                    return new StructRandomGenerator(params, field);
                default:
                    throw new UnsupportedOperationException(String.format("Unsupported type %s.",
                            field.getClass().getSimpleName()));
            }
        }

        private RandomGenerator(PopulateTableParams params, Types.NestedField field) {
            this.params = params;
            this.field = field;
        }

        boolean nextNullable() {
            return RANDOM.nextInt(Integer.MAX_VALUE) * 1.0
                    / Integer.MAX_VALUE < params.nullableRate;
        }

        final Object next() {
            if (field.isOptional() && nextNullable()) {
                return null;
            }
            return doNext();
        }

        protected abstract Object doNext();
    }


    private static final class BooleanRandomGenerator extends RandomGenerator {
        private BooleanRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextBoolean();
        }
    }


    private static final class IntRandomGenerator extends RandomGenerator {
        private IntRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextInt();
        }
    }


    private static final class BigintRandomGenerator extends RandomGenerator {
        private BigintRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextLong();
        }
    }


    private static final class FloatRandomGenerator extends RandomGenerator {
        private FloatRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return 1000000 * RANDOM.nextFloat();
        }
    }


    private static final class DoubleRandomGenerator extends RandomGenerator {
        private DoubleRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextDouble();
        }
    }


    private static final class DateRandomGenerator extends RandomGenerator {
        private DateRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return LocalDate.ofEpochDay(
                    (int) (System.currentTimeMillis() / 1000 / 86400 - RANDOM.nextInt(365)));
        }
    }


    private static final class TimestampRandomGenerator extends RandomGenerator {
        private TimestampRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(
                            System.currentTimeMillis() - RANDOM.nextInt(365 * 86400) * 1000L),
                    ZoneId.systemDefault());
        }
    }


    private static class UUIDRandomGenerator extends RandomGenerator {
        private UUIDRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return UUIDUtils.UUIDToBytes(UUID.randomUUID());
        }
    }


    private static class StringRandomGenerator extends RandomGenerator {
        protected static final String CHARACTERS =
                "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

        private final int length;

        private StringRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
            this.length = (field.type() instanceof Types.FixedType)
                    ? ((Types.FixedType) field.type()).length()
                    : 16;
        }

        @Override
        protected Object doNext() {
            StringBuilder buffer = new StringBuilder(length);
            int randomLength =
                    (field.type() instanceof Types.FixedType) ? length : RANDOM.nextInt(length);
            for (int i = 0; i < randomLength; i++) {
                buffer.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
            }
            return buffer.toString();
        }
    }


    private static final class FixedRandomGenerator extends StringRandomGenerator {
        private FixedRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return ((String) super.doNext()).getBytes(Charset.defaultCharset());
        }
    }


    private static final class BinaryRandomGenerator extends StringRandomGenerator {
        private BinaryRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
        }

        @Override
        protected Object doNext() {
            return ByteBuffer.wrap(((String) super.doNext()).getBytes(Charset.defaultCharset()));
        }
    }


    private static final class DecimalRandomGenerator extends RandomGenerator {
        private final int precision;
        private final int scale;

        private DecimalRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
            this.precision = ((Types.DecimalType) field.type()).precision();
            this.scale = ((Types.DecimalType) field.type()).scale();
        }

        @Override
        protected Object doNext() {
            BigInteger bigInteger = new BigInteger(precision, RANDOM);
            return new BigDecimal(bigInteger, scale);
        }
    }


    private static final class ListRandomGenerator extends RandomGenerator {
        private static final int MAX_LIST_LENGTH = 5;
        private final RandomGenerator elementGenerator;

        private ListRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
            this.elementGenerator = build(params,
                    field.type().asListType().field(field.type().asListType().elementId()));
        }

        boolean nextNullable() {
            return false;
        }

        @Override
        protected Object doNext() {
            List<Object> list = Lists.newArrayList();
            int length = RANDOM.nextInt(MAX_LIST_LENGTH);
            for (int i = 0; i < length; i++) {
                Object nextValue = elementGenerator.next();
                list.add(nextValue);
            }
            return list;
        }
    }


    private static final class MapRandomGenerator extends RandomGenerator {
        private static final int MAX_LENGTH = 5;
        private final RandomGenerator keyGenerator;
        private final RandomGenerator valueGenerator;

        private MapRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
            this.keyGenerator =
                    build(params, field.type().asMapType().field(field.type().asMapType().keyId()));
            this.valueGenerator = build(params,
                    field.type().asMapType().field(field.type().asMapType().valueId()));
        }

        @Override
        protected Object doNext() {
            Map<Object, Object> map = Maps.newHashMap();
            int length = RANDOM.nextInt(MAX_LENGTH);
            while (--length >= 0) {
                map.put(keyGenerator.next(), valueGenerator.next());
            }
            return map;
        }
    }


    private static final class StructRandomGenerator extends RandomGenerator {
        private final List<Types.NestedField> dataFields;
        private final List<RandomGenerator> fieldGenerators;

        private StructRandomGenerator(PopulateTableParams params, Types.NestedField field) {
            super(params, field);
            this.dataFields = field.type().asStructType().fields();
            this.fieldGenerators = this.dataFields.stream()
                    .map(structField -> build(params, structField)).collect(Collectors.toList());
        }

        @Override
        protected Object doNext() {
            GenericRecord record = GenericRecord.create(field.type().asStructType().asSchema());

            for (int i = 0; i < dataFields.size(); i++) {
                RandomGenerator generator = fieldGenerators.get(i);
                Object nextValue = generator.next();
                record.set(i, nextValue);
            }
            return record;
        }
    }
}
