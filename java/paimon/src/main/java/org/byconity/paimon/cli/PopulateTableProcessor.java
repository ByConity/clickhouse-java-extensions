package org.byconity.paimon.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.*;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class PopulateTableProcessor extends ActionProcessor {

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
        Identifier identifier = Identifier.create(params.database, params.table);
        if (!catalog.tableExists(identifier)) {
            System.out.printf("Table '%s.%s' does not exist.\n", params.database, params.table);
            return;
        }
        Table paimonTable = catalog.getTable(identifier);
        RowType rowType = paimonTable.rowType();

        List<RandomGenerator> generators = Lists.newArrayList();
        for (DataField field : rowType.getFields()) {
            generators.add(RandomGenerator.build(params, field.type()));
        }

        BatchWriteBuilder writeBuilder = paimonTable.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            for (int i = 0; i < params.rows; i++) {
                List<Object> values = Lists.newArrayList();
                for (RandomGenerator generator : generators) {
                    values.add(generator.next());
                }
                GenericRow record = GenericRow.of(values.toArray());
                write.write(record);
            }
            List<CommitMessage> messages = write.prepareCommit();
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }

        System.out.printf("Populate table '%s.%s' successfully, %d rows have been generated.\n",
                params.database, params.table, params.rows);
    }

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


    private static abstract class RandomGenerator {
        protected static final Random RANDOM = new Random();
        protected final PopulateTableParams params;
        protected final DataType type;

        static RandomGenerator build(PopulateTableParams params, DataType type) {
            switch (type.getTypeRoot()) {
                case BOOLEAN:
                    return new BooleanRandomGenerator(params, type);
                case TINYINT:
                    return new TinyintRandomGenerator(params, type);
                case SMALLINT:
                    return new SmallintRandomGenerator(params, type);
                case INTEGER:
                    return new IntRandomGenerator(params, type);
                case BIGINT:
                    return new BigintRandomGenerator(params, type);
                case FLOAT:
                    return new FloatRandomGenerator(params, type);
                case DOUBLE:
                    return new DoubleRandomGenerator(params, type);
                case DATE:
                    return new DateRandomGenerator(params, type);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return new TimestampRandomGenerator(params, type);
                case CHAR:
                case VARCHAR:
                    return new StringRandomGenerator(params, type);
                case BINARY:
                case VARBINARY:
                    return new BinaryRandomGenerator(params, type);
                case DECIMAL:
                    return new DecimalRandomGenerator(params, type);
                case ARRAY:
                    return new ArrayRandomGenerator(params, type);
                case MAP:
                    return new MapRandomGenerator(params, type);
                case ROW:
                    return new RowRandomGenerator(params, type);
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported type %s.", type.getClass().getSimpleName()));
            }
        }

        private RandomGenerator(PopulateTableParams params, DataType type) {
            this.params = params;
            this.type = type;
        }

        boolean nextNullable() {
            return RANDOM.nextInt(Integer.MAX_VALUE) * 1.0
                    / Integer.MAX_VALUE < params.nullableRate;
        }

        final Object next() {
            if (type.isNullable() && nextNullable()) {
                return null;
            }
            return doNext();
        }

        protected abstract Object doNext();
    }


    private static final class BooleanRandomGenerator extends RandomGenerator {
        private BooleanRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextBoolean();
        }
    }


    private static final class TinyintRandomGenerator extends RandomGenerator {
        private TinyintRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return (byte) RANDOM.nextInt(128);
        }
    }


    private static final class SmallintRandomGenerator extends RandomGenerator {
        private SmallintRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return (short) RANDOM.nextInt(32768);
        }
    }


    private static final class IntRandomGenerator extends RandomGenerator {
        private IntRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextInt();
        }
    }


    private static final class BigintRandomGenerator extends RandomGenerator {
        private BigintRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextLong();
        }
    }


    private static final class FloatRandomGenerator extends RandomGenerator {
        private FloatRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return 1000000 * RANDOM.nextFloat();
        }
    }


    private static final class DoubleRandomGenerator extends RandomGenerator {
        private DoubleRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return RANDOM.nextDouble();
        }
    }


    private static final class DateRandomGenerator extends RandomGenerator {
        private DateRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return (int) (System.currentTimeMillis() / 1000 / 86400 - RANDOM.nextInt(365));
        }
    }


    private static final class TimeRandomGenerator extends RandomGenerator {
        private TimeRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return (int) (System.currentTimeMillis() / 1000 - RANDOM.nextInt(365 * 86400));
        }
    }


    private static final class TimestampRandomGenerator extends RandomGenerator {
        private TimestampRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected Object doNext() {
            return Timestamp.fromEpochMillis(
                    System.currentTimeMillis() - RANDOM.nextInt(365 * 86400) * 1000L);
        }
    }


    private static class StringRandomGenerator extends RandomGenerator {
        protected static final String CHARACTERS =
                "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

        private final int length;

        private StringRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
            if (CharType.class.equals(type.getClass())) {
                length = ((CharType) type).getLength();
            } else if (VarCharType.class.equals(type.getClass())) {
                length = ((VarCharType) type).getLength();
            } else if (BinaryType.class.equals(type.getClass())) {
                this.length = ((BinaryType) type).getLength();
            } else {
                this.length = ((VarBinaryType) type).getLength();
            }
        }

        @Override
        protected Object doNext() {
            StringBuilder buffer = new StringBuilder(length);
            int randomLength = RANDOM.nextInt(length);
            for (int i = 0; i < randomLength; i++) {
                buffer.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
            }
            return BinaryString.fromString(buffer.toString());
        }
    }


    private static final class BinaryRandomGenerator extends StringRandomGenerator {
        private BinaryRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
        }

        @Override
        protected byte[] doNext() {
            return ((BinaryString) super.doNext()).toBytes();
        }
    }


    private static final class DecimalRandomGenerator extends RandomGenerator {
        private final int precision;
        private final int scale;

        private DecimalRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
            this.precision = ((DecimalType) type).getPrecision();
            this.scale = ((DecimalType) type).getScale();
        }

        @Override
        protected Object doNext() {
            BigInteger bigInteger = new BigInteger(precision, RANDOM);
            BigDecimal bigDecimal = new BigDecimal(bigInteger, scale);
            return Decimal.fromBigDecimal(bigDecimal, precision, scale);
        }
    }


    private static final class BinaryArrayRandomGenerator extends RandomGenerator {
        private final RandomGenerator targetGenerator;
        private final BinaryWriter.ValueSetter valueSetter;
        private final int typeSize;
        private int length;

        private BinaryArrayRandomGenerator(PopulateTableParams params, DataType type,
                RandomGenerator targetGenerator) {
            super(params, type);
            this.targetGenerator = targetGenerator;
            this.valueSetter = BinaryWriter.createValueSetter(type);
            this.typeSize = BinaryArray.calculateFixLengthPartSize(type);
        }

        void setLength(int length) {
            this.length = length;
        }

        boolean nextNullable() {
            return false;
        }

        @Override
        protected Object doNext() {
            BinaryArray binaryArray = new BinaryArray();
            BinaryArrayWriter writer = new BinaryArrayWriter(binaryArray, length, typeSize);
            BinaryArrayWriter.NullSetter nullSetter = BinaryArrayWriter.createNullSetter(type);
            for (int i = 0; i < length; i++) {
                Object nextValue = targetGenerator.next();
                if (nextValue == null) {
                    nullSetter.setNull(writer, i);
                } else {
                    valueSetter.setValue(writer, i, nextValue);
                }
            }
            writer.complete();
            return binaryArray;
        }
    }


    private static final class ArrayRandomGenerator extends RandomGenerator {
        private static final int MAX_LENGTH = 5;
        private final BinaryArrayRandomGenerator elementGenerator;

        private ArrayRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
            this.elementGenerator =
                    new BinaryArrayRandomGenerator(params, ((ArrayType) type).getElementType(),
                            build(params, ((ArrayType) type).getElementType()));
        }

        @Override
        protected Object doNext() {
            elementGenerator.setLength(RANDOM.nextInt(MAX_LENGTH));
            return elementGenerator.next();
        }
    }


    private static final class MapRandomGenerator extends RandomGenerator {
        private static final int MAX_LENGTH = 5;
        private final BinaryArrayRandomGenerator keyGenerator;
        private final BinaryArrayRandomGenerator valueGenerator;

        private MapRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
            this.keyGenerator = new BinaryArrayRandomGenerator(params,
                    ((MapType) type).getKeyType(), build(params, ((MapType) type).getKeyType()));
            this.valueGenerator =
                    new BinaryArrayRandomGenerator(params, ((MapType) type).getValueType(),
                            build(params, ((MapType) type).getValueType()));
        }

        @Override
        protected Object doNext() {
            int length = RANDOM.nextInt(MAX_LENGTH);
            keyGenerator.setLength(length);
            valueGenerator.setLength(length);
            BinaryArray keys = (BinaryArray) keyGenerator.next();
            BinaryArray values = (BinaryArray) valueGenerator.next();
            Preconditions.checkState(keys != null);
            Preconditions.checkState(values != null);
            return BinaryMap.valueOf(keys, values);
        }
    }


    private static final class RowRandomGenerator extends RandomGenerator {
        private final List<DataField> dataFields;
        private final List<RandomGenerator> fieldGenerators;
        private final List<BinaryWriter.ValueSetter> valueSetters;

        private RowRandomGenerator(PopulateTableParams params, DataType type) {
            super(params, type);
            this.dataFields = ((RowType) type).getFields();
            this.fieldGenerators = this.dataFields.stream().map(DataField::type)
                    .map(fieldType -> build(params, fieldType)).collect(Collectors.toList());
            this.valueSetters = this.dataFields.stream().map(DataField::type)
                    .map(BinaryWriter::createValueSetter).collect(Collectors.toList());
        }

        @Override
        protected Object doNext() {
            BinaryRow binaryRow = new BinaryRow(dataFields.size());
            BinaryRowWriter writer = new BinaryRowWriter(binaryRow);

            for (int i = 0; i < dataFields.size(); i++) {
                RandomGenerator generator = fieldGenerators.get(i);
                Object nextValue = generator.next();

                if (nextValue == null) {
                    writer.setNullAt(i);
                } else {
                    BinaryWriter.ValueSetter valueSetter = valueSetters.get(i);
                    valueSetter.setValue(writer, i, nextValue);
                }
            }
            writer.complete();
            return binaryRow;
        }
    }
}
