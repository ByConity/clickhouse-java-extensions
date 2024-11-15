package org.byconity.paimon.util;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class PaimonUtils {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder();
    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    public static <T> byte[] encodeObjectToBytes(T obj) {
        if (obj == null) {
            return null;
        }
        byte[] bytes;
        try {
            bytes = InstantiationUtil.serializeObject(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return BASE64_ENCODER.encode(bytes);
    }

    public static <T> T decodeStringToObject(String encodedStr) {
        if (encodedStr == null) {
            return null;
        }
        final byte[] bytes = BASE64_DECODER.decode(encodedStr.getBytes(UTF_8));
        try {
            return InstantiationUtil.deserializeObject(bytes, PaimonUtils.class.getClassLoader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> fieldNames(RowType rowType) {
        return rowType.getFields().stream().map(DataField::name).collect(Collectors.toList());
    }
}
