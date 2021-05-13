/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.format.Util.readFileMetaData;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

/**
 * Utility functions for parquet file metadata.
 */
public class ParquetUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetUtils.class);

    public static class RequiredRowGroups {
        private int startIndex = 0;
        private int number = 0;

        public int getStartIndex() {
            return startIndex;
        }

        public void setStartIndex(int startIndex) {
            this.startIndex = startIndex;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }
    }

    // get required row groups for a task split.
    public static RequiredRowGroups getRequiredSplitRowGroups(
            FileSourceSplit split, Configuration configuration) throws IOException {

        long splitStart = split.offset();
        long splitSize = split.length();
        InputStream footerBytesStream = getFooterBytesStream(split, configuration);

        RequiredRowGroups ret = filterRowGroupsByMidpoint(
                readFileMetaData(footerBytesStream),
                splitStart,
                splitStart + splitSize);
        footerBytesStream.close();

        return ret;
    }

    private static InputStream getFooterBytesStream(
            FileSourceSplit split, Configuration configuration) throws IOException {

        org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(split.path().toUri());
        HadoopInputFile inputFile = HadoopInputFile.fromPath(file, configuration);

        SeekableInputStream f = inputFile.newStream();
        long fileLen = inputFile.getLength();

        int FOOTER_LENGTH_SIZE = 4;
        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
            // MAGIC + data + footer + footerIndex + MAGIC
            throw new RuntimeException(file.toString() +
                    " is not a Parquet file (too small length: " + fileLen + ")");
        }

        long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;

        f.seek(footerLengthIndex);
        int footerLength = readIntLittleEndian(f);
        byte[] magic = new byte[MAGIC.length];
        f.readFully(magic);

        if (!Arrays.equals(MAGIC, magic)) {
            throw new RuntimeException(file.toString() +
                    " is not a Parquet file. expected magic number at tail "
                    + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
        }

        long footerIndex = footerLengthIndex - footerLength;
        LOG.debug("read footer length: {}, footer index: {}", footerLength, footerIndex);
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
            throw new RuntimeException("corrupted file: the footer index is not within the file: "
                    + footerIndex);
        }

        f.seek(footerIndex);
        ByteBuffer footerBytesBuffer = ByteBuffer.allocate(footerLength);
        f.readFully(footerBytesBuffer);
        f.close();
        LOG.debug("Finished to read all footer bytes.");

        footerBytesBuffer.flip();
        return ByteBufferInputStream.wrap(footerBytesBuffer);
    }

    private static RequiredRowGroups filterRowGroupsByMidpoint(
            FileMetaData metaData, long startOffset, long endOffset) {

        RequiredRowGroups ret = new RequiredRowGroups();

        List<RowGroup> rowGroups = metaData.getRow_groups();
        int inputIndex = 0;
        boolean flag = false;
        for (RowGroup rowGroup : rowGroups) {
            long totalSize = 0;
            long startIndex = getColumnOffset(rowGroup.getColumns().get(0));
            for (ColumnChunk col : rowGroup.getColumns()) {
                totalSize += col.getMeta_data().getTotal_compressed_size();
            }
            long midPoint = startIndex + totalSize / 2;

            if (midPoint >= startOffset && midPoint < endOffset) {
                if (!flag) {
                    ret.startIndex = inputIndex;
                    flag = true;
                }
                ret.number += 1;
            }
            inputIndex++;
        }
        LOG.info("Required row groups: start: {}, num: {}", ret.startIndex, ret.number);

        return ret;
    }

    protected static long getColumnOffset(ColumnChunk columnChunk) {
        ColumnMetaData md = columnChunk.getMeta_data();
        long offset = md.getData_page_offset();
        if (md.isSetDictionary_page_offset() && offset > md.getDictionary_page_offset()) {
            offset = md.getDictionary_page_offset();
        }
        return offset;
    }

}
