package org.byconity.common;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.byconity.common.ArrowSchemaUtils.toArrowField;

public class TestArrowColumnVector {
    static BufferAllocator allocator = new RootAllocator();

    @Test
    public void testIntColumn() {
        ColumnType columnType = new ColumnType("int");
        Field field = toArrowField(columnType, "colInt", true);
        Schema schema = new Schema(Arrays.asList(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        ArrowColumnVector column = new ArrowColumnVector(columnType, 3, root.getVector(0));
        column.putValue(0, new ObjectColumnValue(1));
        column.putValue(1, new ObjectColumnValue(2));
        column.putValue(2, new ObjectColumnValue(3));

        root.setRowCount(3);
        System.out.println(root.contentToTSVString());
    }

    @Test
    public void testArrayColumn() {
        ColumnType columnType = new ColumnType("array<double>");
        Field field = toArrowField(columnType, "colArray", true);
        Schema schema = new Schema(Arrays.asList(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        ArrowColumnVector column = new ArrowColumnVector(columnType, 3, root.getVector(0));

        List<ColumnValue> values = new ArrayList<>();
        values.add(new ObjectColumnValue(1.0));
        column.putValue(0, new ObjectColumnValue(values));

        values.add(new ObjectColumnValue(2.0));
        column.putValue(1, new ObjectColumnValue(values));

        values.add(new ObjectColumnValue(3.0));
        column.putValue(2, new ObjectColumnValue(values));

        root.setRowCount(3);
        System.out.println(root.contentToTSVString());
    }

    @Test
    public void testMapColumn() {
        ColumnType columnType = new ColumnType("map<string,double>");
        Field field = toArrowField(columnType, "colMap", true);
        Schema schema = new Schema(Arrays.asList(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowColumnVector column = new ArrowColumnVector(columnType, 3, root.getVector(0));

        List<ColumnValue> keys = new ArrayList<>();
        keys.add(new ObjectColumnValue("1"));
        keys.add(new ObjectColumnValue("12"));
        keys.add(new ObjectColumnValue("13"));

        List<ColumnValue> values = new ArrayList<>();
        values.add(new ObjectColumnValue(1.0));
        values.add(new ObjectColumnValue(2.0));
        values.add(new ObjectColumnValue(3.0));

        ColumnValue mapValue = new ObjectColumnValue(keys, values);
        column.putValue(0, mapValue);
        root.setRowCount(3);
        System.out.println(root.contentToTSVString());
    }

    @Test
    public void testStructColumn() {
        ColumnType columnType = new ColumnType("struct<a:int,b:string,c:float>");
        SelectedFields selectedFields = new SelectedFields();
        selectedFields.addMultipleNestedPath("a,b,c");
        columnType.pruneOnSelectedFields(selectedFields);

        Field field = toArrowField(columnType, "colMap", true);
        Schema schema = new Schema(Arrays.asList(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowColumnVector column = new ArrowColumnVector(columnType, 3, root.getVector(0));

        List<ColumnValue> values = Arrays.asList(new ObjectColumnValue(1),
                new ObjectColumnValue("abc"), new ObjectColumnValue(2.0f));
        column.putValue(0, new ObjectColumnValue(values));

        values = Arrays.asList(new ObjectColumnValue(2), new ObjectColumnValue("defg"),
                new ObjectColumnValue(2.1f));
        column.putValue(1, new ObjectColumnValue(values));

        values = Arrays.asList(new ObjectColumnValue(3), new ObjectColumnValue("hijk"),
                new ObjectColumnValue(2.2f));
        column.putValue(2, new ObjectColumnValue(values));

        root.setRowCount(3);
        System.out.println(root.contentToTSVString());
    }
}
