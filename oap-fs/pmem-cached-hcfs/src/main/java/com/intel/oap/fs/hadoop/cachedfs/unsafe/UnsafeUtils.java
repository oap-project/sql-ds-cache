package com.intel.oap.fs.hadoop.cachedfs.unsafe;

import com.intel.oap.fs.hadoop.cachedfs.Constants;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtils {
    private static final Unsafe _UNSAFE;
    public static final int BYTE_ARRAY_OFFSET;

    static {
        Unsafe unsafe;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe)unsafeField.get((Object)null);
        } catch (Throwable var4) {
            unsafe = null;
        }

        _UNSAFE = unsafe;
        if (_UNSAFE != null) {
            BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
        } else {
            BYTE_ARRAY_OFFSET = 0;
        }
    }

    public static boolean available() {
        return _UNSAFE != null;
    }

    public static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
        long size;
        if (dstOffset < srcOffset) {
            while(length > 0L) {
                size = Math.min(length, Constants.UNSAFE_COPY_MEMORY_STEP_LENGTH);
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;

            for(dstOffset += length; length > 0L; length -= size) {
                size = Math.min(length, Constants.UNSAFE_COPY_MEMORY_STEP_LENGTH);
                srcOffset -= size;
                dstOffset -= size;
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
            }
        }

    }
}
