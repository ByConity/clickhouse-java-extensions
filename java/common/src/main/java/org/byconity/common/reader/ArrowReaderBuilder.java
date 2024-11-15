package org.byconity.common.reader;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.byconity.common.loader.ThreadContextClassLoader;

import java.io.IOException;

public abstract class ArrowReaderBuilder {

    public void initStream(long streamAddress) throws IOException {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(getClass().getClassLoader())) {
            try (ArrowArrayStream stream = ArrowArrayStream.wrap(streamAddress)) {
                ArrowReader reader = build();
                Data.exportArrayStream(getAllocator(), reader, stream);
            }
        }
    }

    public abstract ArrowReader build() throws IOException;

    public abstract BufferAllocator getAllocator();

}
