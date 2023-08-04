package org.apache.hudi

import org.apache.hadoop.fs.FileStatus
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.metadata.{HoodieTableMetadata, HoodieTableMetadataUtil}
import org.apache.hudi.util.JFunction
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}


class FilePruningSupport(spark: SparkSession,
                         metadataConfig: HoodieMetadataConfig,
                         metaClient: HoodieTableMetaClient) {

  @transient private lazy val engineCtx = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
  @transient private lazy val metadataTable: HoodieTableMetadata =
    HoodieTableMetadata.create(engineCtx, metadataConfig, metaClient.getBasePathV2.toString)


  /**
   * Returns the attribute and literal pair given the operands of a binary operator. The pair is returned only if one of
   * the operand is an attribute and other is literal. In other cases it returns an empty Option.
   * @param expression1 - Left operand of the binary operator
   * @param expression2 - Right operand of the binary operator
   * @return Attribute and literal pair
   */
  private def getAttributeLiteralTuple(expression1: Expression, expression2: Expression): Option[(AttributeReference, Literal)] = {
    expression1 match {
      case attr: AttributeReference => expression2 match {
        case literal: Literal =>
          Option.apply(attr, literal)
        case _ =>
          Option.empty
      }
      case literal: Literal => expression2 match {
        case attr: AttributeReference =>
          Option.apply(attr, literal)
        case _ =>
          Option.empty
      }
      case _ => Option.empty
    }

  }

  /**
   * Given query filters, it filters the EqualTo queries on simple record key columns and returns a tuple of list of such
   * queries and list of record key literals present in the query.
   * @param queryFilters The queries that need to be filtered.
   * @return Tuple of List of filtered queries and list of record key literals that need to be matched
   */
  def sliceFiltersForColStatsAndRecordLevelIndex(queryFilters: Seq[Expression], queryReferencedColumns: Seq[String]): (Seq[Expression], Seq[Expression]) = {
    var colStatsQueryFilters: Seq[Expression] = Seq.empty
    var recordLevelIndexQueryFilters: Seq[Expression] = Seq.empty
    var recordLevelIndexQueryRefCols: Seq[String] = Seq.empty
    for (query <- queryFilters) {
      query match {
        case equalToQuery: EqualTo =>
          val (attribute, literal) = getAttributeLiteralTuple(equalToQuery.left, equalToQuery.right).orNull
          if (attribute != null && attribute.name != null && attributeMatchesRecordKey(attribute.name)) {
            recordLevelIndexQueryFilters = recordLevelIndexQueryFilters :+ query
          } else {
            colStatsQueryFilters = colStatsQueryFilters :+ query
          }
        case _ =>
          colStatsQueryFilters = colStatsQueryFilters :+ query
      }
    }
    Tuple2.apply(colStatsQueryFilters, recordLevelIndexQueryFilters)
  }

  /**
   * Matches the configured simple record key with the input attribute name.
   * @param attributeName The attribute name provided in the query
   * @return true if input attribute name matches the configured simple record key
   */
  private def attributeMatchesRecordKey(attributeName: String): Boolean = {
    val recordKeyOpt = getRecordKeyConfig
    if (recordKeyOpt.isDefined && recordKeyOpt.get == attributeName) {
      true
    } else {
      HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName == recordKeyOpt.get
    }
  }

  private def getRecordKeyConfig: Option[String] = {
    val recordKeysOpt: org.apache.hudi.common.util.Option[Array[String]] = metaClient.getTableConfig.getRecordKeyFields
    val recordKeyOpt = recordKeysOpt.map[String](JFunction.toJavaFunction[Array[String], String](arr =>
      if (arr.length == 1) {
        arr(0)
      } else {
        null
      }))
    Option.apply(recordKeyOpt.orElse(null))
  }

  def isIndexAvailable: Boolean = {
    isRecordLevelIndexAvailable || isColStatsIndexAvailable
  }

  def isRecordLevelIndexAvailable: Boolean = {
    metadataConfig.enabled && metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
  }
  /**
   * Returns true in cases when Column Stats Index is built and available as standalone partition
   * w/in the Metadata Table
   */
  def isColStatsIndexAvailable: Boolean = {
    checkState(metadataConfig.enabled, "Metadata Table support has to be enabled")
    metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
  }
}
