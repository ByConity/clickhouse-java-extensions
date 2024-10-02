package org.byconity.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SelectedFields {
    private final Set<String> dedup = new HashSet<>();
    private final List<String> fields = new ArrayList<>();
    private Map<String, SelectedFields> children = null;

    // add nested path like 'a.b.c.d' to nested fields
    public void addNestedPath(String path) {
        String[] paths = path.split("\\.");
        addNestedPath(paths, 0);
    }

    public void addMultipleNestedPath(String path) {
        for (String p : path.split(",")) {
            addNestedPath(p);
        }
    }

    public void addNestedPath(String[] paths, int offset) {
        String f = paths[offset];
        if (!dedup.contains(f)) {
            fields.add(f);
            dedup.add(f);
        }
        if ((offset + 1) < paths.length) {
            if (children == null) {
                children = new HashMap<>();
            }
            if (!children.containsKey(f)) {
                SelectedFields sub = new SelectedFields();
                children.put(f, sub);
            }
            children.get(f).addNestedPath(paths, offset + 1);
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public SelectedFields findChildren(String f) {
        return children == null ? null : children.get(f);
    }
}
