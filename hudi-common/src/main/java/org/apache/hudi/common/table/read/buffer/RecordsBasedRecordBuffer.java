package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieNotSupportedException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class RecordsBasedRecordBuffer<T> extends FileGroupRecordBuffer<T> {
  private final Map<Serializable, BufferedRecord<T>> existingRecords;

  RecordsBasedRecordBuffer(HoodieReaderContext<T> readerContext, HoodieTableMetaClient hoodieTableMetaClient,
                           RecordMergeMode recordMergeMode, PartialUpdateMode partialUpdateMode,
                           TypedProperties props, List<String> orderingFieldNames,
                           UpdateProcessor<T> updateProcessor, Map<Serializable, BufferedRecord<T>> records) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, orderingFieldNames, updateProcessor);
    this.existingRecords = records;
  }

  @Override
  protected ExternalSpillableMap<Serializable, BufferedRecord<T>> initializeRecordsMap(String spillableMapBasePath) {
    return null;
  }

  @Override
  protected void initializeLogRecordIterator() {
    logRecordIterator = existingRecords.values().iterator();
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Handle merging.
    while (baseFileIterator.hasNext()) {
      if (hasNextBaseRecord(baseFileIterator.next())) {
        return true;
      }
    }

    // Handle records solely from log files.
    return hasNextLogRecord();
  }

  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema);
    // Avoid removing from the map so the map can be reused later
    BufferedRecord<T> logRecordInfo = existingRecords.get(recordKey);
    return hasNextBaseRecord(baseRecord, logRecordInfo);
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) {
    throw new HoodieNotSupportedException("Reusable record buffer does not perform the processing of the data blocks");
  }

  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable index) {
    throw new HoodieNotSupportedException("Reusable record buffer does not process the data records from the logs");

  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) {
    throw new HoodieNotSupportedException("Reusable record buffer does not perform the processing of the delete blocks");

  }

  @Override
  public void processNextDeletedRecord(DeleteRecord record, Serializable index) {
    throw new HoodieNotSupportedException("Reusable record buffer does not process the delete records from the logs");
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return existingRecords.containsKey(recordKey);
  }

  /**
   * The close method is a no-op for this buffer implementation since the record map is managed by the {@link ReusableFileGroupRecordBufferLoader<T>}.
   */
  @Override
  public void close() {
    // no-op
  }
}
