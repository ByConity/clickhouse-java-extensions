// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.byconity.paimon.util;

import org.apache.paimon.types.*;

import java.util.stream.Collectors;

/**
 * Convert paimon type to hive string representation.
 */
public abstract class PaimonTypeUtils {

    public static String fromPaimonType(DataType type) {
        return type.accept(PaimonToHiveTypeVisitor.INSTANCE);
    }

    private static class PaimonToHiveTypeVisitor extends DataTypeDefaultVisitor<String> {

        private static final PaimonToHiveTypeVisitor INSTANCE = new PaimonToHiveTypeVisitor();

        public String visit(CharType charType) {
            return "string";
        }

        public String visit(VarCharType varCharType) {
            return "string";
        }

        public String visit(BooleanType booleanType) {
            return "boolean";
        }

        public String visit(BinaryType binaryType) {
            return "binary";
        }

        public String visit(VarBinaryType varBinaryType) {
            return "binary";
        }

        public String visit(DecimalType decimalType) {
            return String.format("decimal(%d,%d)", decimalType.getPrecision(),
                    decimalType.getScale());
        }

        public String visit(TinyIntType tinyIntType) {
            return "tinyint";
        }

        public String visit(SmallIntType smallIntType) {
            return "short";
        }

        public String visit(IntType intType) {
            return "int";
        }

        public String visit(BigIntType bigIntType) {
            return "bigint";
        }

        public String visit(FloatType floatType) {
            return "float";
        }

        public String visit(DoubleType doubleType) {
            return "double";
        }

        public String visit(DateType dateType) {
            return "date";
        }

        public String visit(TimeType timeType) {
            return "timestamp";
        }

        public String visit(TimestampType timestampType) {
            return "timestamp-millis";
        }

        public String visit(LocalZonedTimestampType timestampType) {
            return "timestamp-millis";
        }

        public String visit(ArrayType arrayType) {
            return String.format("array<%s>", arrayType.getElementType().accept(this));
        }

        public String visit(MapType mapType) {
            return String.format("map<%s,%s>", mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this));
        }

        public String visit(RowType rowType) {
            String type =
                    rowType.getFields().stream().map(f -> f.name() + ":" + f.type().accept(this))
                            .collect(Collectors.joining(","));
            return String.format("struct<%s>", type);
        }

        @Override
        protected String defaultMethod(DataType dataType) {
            return dataType.getTypeRoot().name();
        }
    }
}
