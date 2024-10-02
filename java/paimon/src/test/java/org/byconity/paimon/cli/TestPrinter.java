package org.byconity.paimon.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class TestPrinter extends PrintStream {

    private final ByteArrayOutputStream out;

    public TestPrinter(ByteArrayOutputStream out) {
        super(out);
        this.out = out;
    }

    public void reset() {
        out.reset();
    }

    public String getContent() {
        return out.toString();
    }
}
