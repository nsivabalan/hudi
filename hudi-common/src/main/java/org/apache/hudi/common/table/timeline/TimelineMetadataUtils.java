/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utils for Hudi timeline metadata.
 */
public class TimelineMetadataUtils {

  private static final Integer DEFAULT_VERSION = 1;

  public static HoodieRestoreMetadata convertRestoreMetadata(String startRestoreTime,
                                                             long durationInMs,
                                                             List<HoodieInstant> instants,
                                                             Map<String, List<HoodieRollbackMetadata>> instantToRollbackMetadata) {
    return new HoodieRestoreMetadata(startRestoreTime, durationInMs,
        instants.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList()),
        Collections.unmodifiableMap(instantToRollbackMetadata), DEFAULT_VERSION,
        instants.stream().map(instant -> new HoodieInstantInfo(instant.requestedTime(), instant.getAction())).collect(Collectors.toList()));
  }

  public static HoodieRollbackMetadata convertRollbackMetadata(String startRollbackTime, Option<Long> durationInMs,
                                                               List<HoodieInstant> instants, List<HoodieRollbackStat> rollbackStats) {
    Map<String, HoodieRollbackPartitionMetadata> partitionMetadataBuilder = new HashMap<>();
    int totalDeleted = 0;
    for (HoodieRollbackStat stat : rollbackStats) {
      Map<String, Long> rollbackLogFiles = stat.getCommandBlocksCount().keySet().stream()
          .collect(Collectors.toMap(f -> f.getPath().toString(), StoragePathInfo::getLength));
      HoodieRollbackPartitionMetadata metadata = new HoodieRollbackPartitionMetadata(stat.getPartitionPath(),
          stat.getSuccessDeleteFiles(), stat.getFailedDeleteFiles(), rollbackLogFiles, stat.getLogFilesFromFailedCommit());
      partitionMetadataBuilder.put(stat.getPartitionPath(), metadata);
      totalDeleted += stat.getSuccessDeleteFiles().size();
    }

    return new HoodieRollbackMetadata(startRollbackTime, durationInMs.orElseGet(() -> -1L), totalDeleted,
        instants.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList()),
        Collections.unmodifiableMap(partitionMetadataBuilder), DEFAULT_VERSION,
        instants.stream().map(instant -> new HoodieInstantInfo(instant.requestedTime(), instant.getAction())).collect(Collectors.toList()));
  }

  public static HoodieSavepointMetadata convertSavepointMetadata(String user, String comment,
                                                                 Map<String, List<String>> latestFiles) {
    Map<String, HoodieSavepointPartitionMetadata> partitionMetadataBuilder = new HashMap<>();
    for (Map.Entry<String, List<String>> stat : latestFiles.entrySet()) {
      HoodieSavepointPartitionMetadata metadata = new HoodieSavepointPartitionMetadata(stat.getKey(), stat.getValue());
      partitionMetadataBuilder.put(stat.getKey(), metadata);
    }
    return new HoodieSavepointMetadata(user, System.currentTimeMillis(), comment,
        Collections.unmodifiableMap(partitionMetadataBuilder), DEFAULT_VERSION);
  }

  public static Option<byte[]> serializeCompactionPlan(HoodieCompactionPlan compactionWorkload) throws IOException {
    return serializeAvroMetadata(compactionWorkload, HoodieCompactionPlan.class);
  }

  public static Option<byte[]> serializeCleanerPlan(HoodieCleanerPlan cleanPlan) throws IOException {
    return serializeAvroMetadata(cleanPlan, HoodieCleanerPlan.class);
  }

  public static Option<byte[]> serializeRollbackPlan(HoodieRollbackPlan rollbackPlan) throws IOException {
    return serializeAvroMetadata(rollbackPlan, HoodieRollbackPlan.class);
  }

  public static Option<byte[]> serializeRestorePlan(HoodieRestorePlan restorePlan) throws IOException {
    return serializeAvroMetadata(restorePlan, HoodieRestorePlan.class);
  }

  public static Option<byte[]> serializeCleanMetadata(HoodieCleanMetadata metadata) throws IOException {
    return serializeAvroMetadata(metadata, HoodieCleanMetadata.class);
  }

  public static Option<byte[]> serializeSavepointMetadata(HoodieSavepointMetadata metadata) throws IOException {
    return serializeAvroMetadata(metadata, HoodieSavepointMetadata.class);
  }

  public static Option<byte[]> serializeRollbackMetadata(HoodieRollbackMetadata rollbackMetadata) throws IOException {
    return serializeAvroMetadata(rollbackMetadata, HoodieRollbackMetadata.class);
  }

  public static Option<byte[]> serializeRestoreMetadata(HoodieRestoreMetadata restoreMetadata) throws IOException {
    return serializeAvroMetadata(restoreMetadata, HoodieRestoreMetadata.class);
  }

  public static Option<byte[]> serializeRequestedReplaceMetadata(HoodieRequestedReplaceMetadata clusteringPlan) throws IOException {
    return serializeAvroMetadata(clusteringPlan, HoodieRequestedReplaceMetadata.class);
  }

  public static Option<byte[]> serializeIndexPlan(HoodieIndexPlan indexPlan) throws IOException {
    return serializeAvroMetadata(indexPlan, HoodieIndexPlan.class);
  }

  public static Option<byte[]> serializeIndexCommitMetadata(HoodieIndexCommitMetadata indexCommitMetadata) throws IOException {
    return serializeAvroMetadata(indexCommitMetadata, HoodieIndexCommitMetadata.class);
  }

  public static Option<byte[]> serializeCommitMetadata(CommitMetadataSerDe commitMetadataSerDe,
                                                       org.apache.hudi.common.model.HoodieCommitMetadata commitMetadata) throws IOException {
    return commitMetadataSerDe.serialize(commitMetadata);
  }

  public static <T extends SpecificRecordBase> Option<byte[]> serializeAvroMetadata(T metadata, Class<T> clazz)
      throws IOException {
    DatumWriter<T> datumWriter = new SpecificDatumWriter<>(clazz);
    try (DataFileWriter<T> fileWriter = new DataFileWriter<>(datumWriter)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      fileWriter.create(metadata.getSchema(), baos);
      fileWriter.append(metadata);
      fileWriter.flush();
      return Option.of(baos.toByteArray());
    }
  }

  public static <T extends SpecificRecordBase> T deserializeAvroMetadataLegacy(byte[] bytes, Class<T> clazz)
      throws IOException {
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
    FileReader<T> fileReader = DataFileReader.openReader(new SeekableByteArrayInput(bytes), reader);
    ValidationUtils.checkArgument(fileReader.hasNext(), "Could not deserialize metadata of type " + clazz);
    return fileReader.next();
  }

  public static <T extends SpecificRecordBase> T deserializeAvroMetadata(InputStream inputStream, Class<T> clazz)
      throws IOException {
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
    try (DataFileStream<T> fileReader = new DataFileStream<>(inputStream, reader)) {
      ValidationUtils.checkArgument(fileReader.hasNext(), "Could not deserialize metadata of type " + clazz);
      return fileReader.next();
    }
  }
}
