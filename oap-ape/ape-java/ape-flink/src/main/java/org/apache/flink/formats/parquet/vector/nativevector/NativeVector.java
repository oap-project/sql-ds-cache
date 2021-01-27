package org.apache.flink.formats.parquet.vector.nativevector;

public interface NativeVector {
    void setPtr(long bufferPtr_, long nullPtr_, int size_);
    long getBufferPtr();
    long getNullPtr();
}
