/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.{DataSourceReadOptions, HoodieDataSourceHelpers, HoodieSparkUtils, ScalaAssertionSupport}
import org.apache.spark.sql.internal.SQLConf

class TestMergeIntoTable extends HoodieSparkSqlTestBase with ScalaAssertionSupport {

  test("Test MergeInto Basic") {
    Seq(true, false).foreach { sparkSqlOptimizedWrites =>
      withRecordType()(withTempDir { tmp =>
        spark.sql("set hoodie.payload.combined.schema.validate = false")
        val tableName = generateTableName
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts int
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        // test with optimized sql merge enabled / disabled.
        spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=$sparkSqlOptimizedWrites")

        // First merge with a extra input field 'flag' (insert a new record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '1' as flag
             | ) s0
             | on s0.id = $tableName.id
             | when matched and flag = '1' then update set
             | id = s0.id, name = s0.name, price = s0.price, ts = s0.ts + 1
             | when not matched and flag = '1' then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // Second merge (update the record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1001 as ts
             | ) s0
             | on s0.id = $tableName.id
             | when matched then update set
             | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
             | when not matched then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 20.0, 1001)
        )

        // the third time merge (update & insert the record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select * from (
             |  select 1 as id, 'a1' as name, 10 as price, 1002 as ts
             |  union all
             |  select 2 as id, 'a2' as name, 12 as price, 1001 as ts
             |  )
             | ) s0
             | on s0.id = $tableName.id
             | when matched then update set
             | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
             | when not matched and s0.id % 2 = 0 then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 30.0, 1002),
          Seq(2, "a2", 12.0, 1001)
        )

        // the fourth merge (delete the record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 12 as price, 1003 as ts
             | ) s0
             | on s0.id = $tableName.id
             | when matched and s0.id != 1 then update set
             |    id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
             | when matched and s0.id = 1 then delete
             | when not matched then insert *
       """.stripMargin)
        val cnt = spark.sql(s"select * from $tableName where id = 1").count()
        assertResult(0)(cnt)
      })
    }
  }

  /**
   * In Spark 3.0.x, UPDATE and DELETE can appear at most once in MATCHED clauses in a MERGE INTO statement.
   * Refer to: `org.apache.spark.sql.catalyst.parser.AstBuilder#visitMergeIntoTable`
   *
   */
  test("Test MergeInto with more than once update actions for spark >= 3.1.x") {

    if (HoodieSparkUtils.gteqSpark3_1) {
      withRecordType()(withTempDir { tmp =>
        val targetTable = generateTableName
        spark.sql(
          s"""
             |create table ${targetTable} (
             |  id int,
             |  name string,
             |  data int,
             |  country string,
             |  ts int
             |) using hudi
             |tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             |partitioned by (country)
             |location '${tmp.getCanonicalPath}/$targetTable'
             |""".stripMargin)
        spark.sql(
          s"""
             |merge into ${targetTable} as target
             |using (
             |select 1 as id, 'lb' as name, 6 as data, 'shu' as country, 1646643193 as ts
             |) source
             |on source.id = target.id
             |when matched then
             |update set *
             |when not matched then
             |insert *
             |""".stripMargin)
        spark.sql(
          s"""
             |merge into ${targetTable} as target
             |using (
             |select 1 as id, 'lb' as name, 5 as data, 'shu' as country, 1646643196 as ts
             |) source
             |on source.id = target.id
             |when matched and source.data > target.data then
             |update set target.data = source.data, target.ts = source.ts
             |when matched and source.data = 5 then
             |update set target.data = source.data, target.ts = source.ts
             |when not matched then
             |insert *
             |""".stripMargin)

        checkAnswer(s"select id, name, data, country, ts from $targetTable")(
          Seq(1, "lb", 5, "shu", 1646643196L)
        )
      })
    }
  }

  /**
   * Test MIT with global index.
   * HUDI-7131
   */
  test("Test Merge Into with Global Index") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withRecordType()(withTempDir { tmp =>
        withSQLConf("hoodie.index.type" -> "GLOBAL_BLOOM") {
          val targetTable = generateTableName
          spark.sql(
            s"""
               |create table ${targetTable} (
               |  id int,
               |  version int,
               |  name string,
               |  inc_day string
               |) using hudi
               |tblproperties (
               |  type = 'cow',
               |  primaryKey = 'id'
               | )
               |partitioned by (inc_day)
               |location '${tmp.getCanonicalPath}/$targetTable'
               |""".stripMargin)
          spark.sql(
            s"""
               |merge into ${targetTable} as target
               |using (
               |select 1 as id, 1 as version, 'str_1' as name, '2023-10-01' as inc_day
               |) source
               |on source.id = target.id
               |when matched then
               |update set *
               |when not matched then
               |insert *
               |""".stripMargin)
          spark.sql(
            s"""
               |merge into ${targetTable} as target
               |using (
               |select 1 as id, 2 as version, 'str_2' as name, '2023-10-01' as inc_day
               |) source
               |on source.id = target.id
               |when matched then
               |update set *
               |when not matched then
               |insert *
               |""".stripMargin)

          checkAnswer(s"select id, version, name, inc_day from $targetTable")(
            Seq(1, 2, "str_2", "2023-10-01")
          )
          // migrate the record to a new partition.

          spark.sql(
            s"""
               |merge into ${targetTable} as target
               |using (
               |select 1 as id, 2 as version, 'str_2' as name, '2023-10-02' as inc_day
               |) source
               |on source.id = target.id
               |when matched then
               |update set *
               |when not matched then
               |insert *
               |""".stripMargin)

          checkAnswer(s"select id, version, name, inc_day from $targetTable")(
            Seq(1, 2, "str_2", "2023-10-02")
          )
        }
      })
      spark.sessionState.conf.unsetConf("hoodie.index.type")
    }
  }

  test("Test MergeInto with ignored record") {
    withRecordType()(withTempDir {tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val sourceTable = generateTableName
      val targetTable = generateTableName
      // Create source table
      spark.sql(
        s"""
           | create table $sourceTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts int
           | ) using parquet
           | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
      // Create target table
      spark.sql(
        s"""
           |create table $targetTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts int
           |) using hudi
           | location '${tmp.getCanonicalPath}/$targetTable'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // Insert data to source table
      spark.sql(s"insert into $sourceTable values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $sourceTable values(2, 'a2', 11, 1000)")

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using (select * from $sourceTable) as s0
           | on t0.id = s0.id
           | when matched then update set *
           | when not matched and s0.name = 'a1' then insert *
         """.stripMargin)
      // The record of "name = 'a2'" will be filter
      checkAnswer(s"select id, name, price, ts from $targetTable")(
        Seq(1, "a1", 10.0, 1000)
      )

      spark.sql(s"insert into $targetTable select 3, 'a3', 12, 1000")
      checkAnswer(s"select id, name, price, ts from $targetTable")(
        Seq(1, "a1", 10.0, 1000),
        Seq(3, "a3", 12, 1000)
      )

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using (
           |  select * from (
           |    select 1 as s_id, 'a1' as name, 20 as price, 1001 as ts
           |    union all
           |    select 3 as s_id, 'a3' as name, 20 as price, 1003 as ts
           |    union all
           |    select 4 as s_id, 'a4' as name, 10 as price, 1004 as ts
           |  )
           | ) s0
           | on s0.s_id = t0.id
           | when matched and s0.ts = 1001 then update set id = s0.s_id, name = t0.name, price =
           | s0.price, ts = s0.ts
         """.stripMargin
      )
      // Ignore the update for id = 3
      checkAnswer(s"select id, name, price, ts from $targetTable")(
        Seq(1, "a1", 20.0, 1001),
        Seq(3, "a3", 12.0, 1000)
      )
    })
  }

  test("Test MergeInto with changing partition and global index") {
    Seq(true, false).foreach { updatePartitionPathEnabled =>
      withTempDir { tmp =>
        withSQLConf("hoodie.index.type" -> "GLOBAL_SIMPLE",
          "hoodie.simple.index.update.partition.path" -> updatePartitionPathEnabled.toString) {
          Seq("cow","mor").foreach { tableType =>
            val targetTable = generateTableName
            spark.sql(
              s"""
                 | create table $targetTable (
                 |  id int,
                 |  version int,
                 |  mergeCond string,
                 |  partition string
                 | ) using hudi
                 | partitioned by (partition)
                 | tblproperties (
                 |    'primaryKey' = 'id',
                 |    'type' = '$tableType',
                 |    'payloadClass' = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
                 |    preCombineField = 'version'
                 | )
                 | location '${tmp.getCanonicalPath}/$targetTable'
             """.stripMargin)

            spark.sql(s"insert into $targetTable values(1, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(2, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(3, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(4, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(5, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(6, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(7, 3, 'insert', '2023-10-01')")

            val sourceTable = generateTableName
            spark.sql(
              s"""
                 | create table $sourceTable (
                 | id int,
                 | version int,
                 | mergeCond string,
                 | partition string
                 | ) using parquet
                 | partitioned by (partition)
                 | location '${tmp.getCanonicalPath}/$sourceTable'
            """.stripMargin)

            //merge cond matches and partition is changed
            spark.sql(s"insert into $sourceTable values(1, 2, 'yes', '2023-10-02')")
            //merge cond does not match and partition is changed
            spark.sql(s"insert into $sourceTable values(2, 2, 'no', '2023-10-02')")
            //merge cond matches and partition is not changed
            spark.sql(s"insert into $sourceTable values(3, 2, 'yes', '2023-10-01')")
            //merge cond does not match and partition is not changed
            spark.sql(s"insert into $sourceTable values(4, 2, 'no', '2023-10-01')")
            //merge cond is delete and partition is changed
            spark.sql(s"insert into $sourceTable values(5, 2, 'delete', '2023-10-02')")
            //merge cond is delete and partition is not changed
            spark.sql(s"insert into $sourceTable values(6, 2, 'delete', '2023-10-01')")
            //merge cond matches and order value is smaller
            spark.sql(s"insert into $sourceTable values(7, 2, 'yes', '2023-10-02')")
            //id does not match
            spark.sql(s"insert into $sourceTable values(8, 1, 'insert', '2023-10-01')")

            spark.sql(
              s"""
                 | merge into $targetTable t using
                 | (select * from $sourceTable) as s
                 | on t.id=s.id
                 | when matched and s.mergeCond = 'yes' then update set *
                 | when matched and s.mergeCond = 'delete' then delete
                 | when not matched then insert *
             """.stripMargin)
             val updatedPartitionPath = if (updatePartitionPathEnabled) "partition=2023-10-02" else "partition=2023-10-01"
            checkAnswer(s"select id,version,_hoodie_partition_path from $targetTable order by id")(
              Seq(1, 2, updatedPartitionPath),
              Seq(2, 1, "partition=2023-10-01"),
              Seq(3, 2, "partition=2023-10-01"),
              Seq(4, 1, "partition=2023-10-01"),
              Seq(7, 3, "partition=2023-10-01"),
              Seq(8, 1, "partition=2023-10-01"))
          }
        }
      }
    }
  }

  test("Test MergeInto for MOR table ") {
    withRecordType()(withTempDir {tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      // Create a mor partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
           |  dt string
           | ) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)
      // Insert data
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )
      // Update data when matched-condition not matched.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 12 as price, 1001 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 0 then update set *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )
      // Update data when matched-condition matched.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 12 as price, 1001 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 12, "2021-03-21")
      )
      // Insert a new data.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 0 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 12, "2021-03-21"),
        Seq(2, "a2", 10, "2021-03-21")
      )
      // Update with different source column names.
      spark.sql(
        s"""
           | merge into $tableName t0
           | using (
           |  select 2 as s_id, 'a2' as s_name, 15 as s_price, 1001 as s_ts, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id
           | when matched and s_ts = 1001
           | then update set id = s_id, name = s_name, price = s_price, ts = s_ts, t0.dt = s0.dt
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 12, "2021-03-21"),
        Seq(2, "a2", 15, "2021-03-21")
      )

      // Delete with condition expression.
      val errorMessage = if (HoodieSparkUtils.gteqSpark3_2) {
        "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key and partition path column. Found `t0.id = (s0.s_id + 1)`"
      } else if (HoodieSparkUtils.gteqSpark3_1) {
        "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key and partition path column. Found `t0.`id` = (s0.`s_id` + 1)`"
      } else {
        "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key and partition path column. Found `t0.`id` = (s0.`s_id` + 1)`;"
      }

      checkException(
        s"""
           | merge into $tableName t0
           | using (
           |  select 1 as s_id, 'a2' as s_name, 15 as s_price, 1001 as s_ts, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id + 1
           | when matched and s_ts = 1001 then delete
         """.stripMargin
      )(errorMessage)

      spark.sql(
        s"""
           | merge into $tableName t0
           | using (
           |  select 2 as s_id, 'a2' as s_name, 15 as s_price, 1001 as ts, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id
           | when matched and s0.ts = 1001 then delete
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 12, "2021-03-21")
      )
    })
  }

  test("Test MergeInto with insert only") {
    withRecordType()(withTempDir {tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      // Create a partitioned mor table
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  dt string
           | ) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)

      spark.sql(s"insert into $tableName select 1, 'a1', 10, '2021-03-21'")
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-20' as dt
           | ) s0
           | on s0.id = t0.id
           | when not matched and s0.id % 2 = 0 then insert (id,name,price,dt)
           | values(s0.id,s0.name,s0.price,s0.dt)
         """.stripMargin)
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 10, "2021-03-21"),
        Seq(2, "a2", 10, "2021-03-20")
      )

      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 3 as id, 'a3' as name, 10 as price, 1000 as ts, '2021-03-20' as dt
           | ) s0
           | on s0.id = t0.id
           | when not matched and s0.id % 2 = 0 then insert (id,name,price,dt)
           | values(s0.id,s0.name,s0.price,s0.dt)
         """.stripMargin)
      // id = 3 should not write to the table as it has filtered by id % 2 = 0
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 10, "2021-03-21"),
        Seq(2, "a2", 10, "2021-03-20")
      )
    })
  }

  test("Test MergeInto For PreCombineField") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      Seq("cow", "mor").foreach { tableType =>
        val tableName1 = generateTableName
        // Create a mor partitioned table.
        spark.sql(
          s"""
             | create table $tableName1 (
             |  id int,
             |  name string,
             |  price double,
             |  v int,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'v',
             |  hoodie.compaction.payload.class = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
        // Insert data
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1001 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
        )
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 10, "2021-03-21", 1001)
        )

        // Update data with a smaller version value
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 11 as price, 1000 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
        )
        // Update failed as v = 1000 < 1001
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 10, "2021-03-21", 1001)
        )

        // Update data with a bigger version value
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 12 as price, 1002 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
        )
        // Update success
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 12, "2021-03-21", 1002)
        )
      }
    })
  }

  test("Test MergeInto with preCombine field expression") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      Seq("cow", "mor").foreach { tableType =>
        val tableName1 = generateTableName
        spark.sql(
          s"""
             | create table $tableName1 (
             |  id int,
             |  name string,
             |  price double,
             |  v int,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'v'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
        // Insert data
        spark.sql(s"""insert into $tableName1 values(1, 'a1', 10, 1000, '2021-03-21')""")

        //
        // Update data with a value expression on preCombine field
        // 1) set source column name to be same as target column
        //
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 11 as price, 999 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when matched then update set id=s0.id, name=s0.name, price=s0.price*2, v=s0.v+2, dt=s0.dt
         """.stripMargin
        )
        // Update success as new value 1001 is bigger than original value 1000
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 22, "2021-03-21", 1001)
        )

        //
        // 2) set source column name to be different with target column
        //
        val errorMessage = if (HoodieSparkUtils.gteqSpark3_1) {
          "Failed to resolve pre-combine field `v` w/in the source-table output"
        } else {
          "Failed to resolve pre-combine field `v` w/in the source-table output;"
        }

        checkException(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as s_id, 'a1' as s_name, 12 as s_price, 1000 as s_v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.s_id
             | when matched then update set id=s0.s_id, name=s0.s_name, price=s0.s_price*2, v=s0.s_v+2, dt=s0.dt
         """.stripMargin
        )(errorMessage)

        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as s_id, 'a1' as s_name, 12 as s_price, 1000 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.s_id
             | when matched then update set id=s0.s_id, name=s0.s_name, price=s0.s_price*2, v=s0.v+2, dt=s0.dt
         """.stripMargin
        )
        // Update success as new value 1002 is bigger than original value 1001
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 24, "2021-03-21", 1002)
        )
      }
    })
  }

  test("Test MergeInto with primaryKey expression") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName1 = generateTableName
      spark.sql(
        s"""
           | create table $tableName1 (
           |  id int,
           |  name string,
           |  price double,
           |  v int,
           |  dt string
           | ) using hudi
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'v'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
      // Insert data
      spark.sql(s"""insert into $tableName1 values(3, 'a3', 30, 3000, '2021-03-21')""")
      spark.sql(s"""insert into $tableName1 values(2, 'a2', 20, 2000, '2021-03-21')""")
      spark.sql(s"""insert into $tableName1 values(1, 'a1', 10, 1000, '2021-03-21')""")

      //
      // Delete data with a condition expression on primaryKey field
      // 1) set source column name to be same as target column
      //
      val complexConditionsErrorMessage = if (HoodieSparkUtils.gteqSpark3_2) {
        "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key and partition path column. Found `t0.id = (s0.id + 1)`"
      } else if (HoodieSparkUtils.gteqSpark3_1) {
        "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key and partition path column. Found `t0.`id` = (s0.`id` + 1)`"
      } else {
        "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key and partition path column. Found `t0.`id` = (s0.`id` + 1)`;"
      }

      checkException(
        s"""merge into $tableName1 t0
           | using (
           |  select 1 as id, 'a1' as name, 15 as price, 1001 as v, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.id + 1
           | when matched then delete
       """.stripMargin)(complexConditionsErrorMessage)

      spark.sql(
        s"""
           | merge into $tableName1 t0
           | using (
           |  select 2 as id, 'a2' as name, 20 as price, 2000 as v, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.id
           | when matched then delete
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,v,dt from $tableName1 order by id")(
        Seq(1, "a1", 10, 1000, "2021-03-21"),
        Seq(3, "a3", 30, 3000, "2021-03-21")
      )

      //
      // 2.a) set source column name to be different with target column (should fail unable to match pre-combine field)
      //
      val failedToResolveErrorMessage = if (HoodieSparkUtils.gteqSpark3_1) {
        "Failed to resolve pre-combine field `v` w/in the source-table output"
      } else {
        "Failed to resolve pre-combine field `v` w/in the source-table output;"
      }

      checkException(
        s"""merge into $tableName1 t0
           | using (
           |  select 3 as s_id, 'a3' as s_name, 30 as s_price, 3000 as s_v, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id
           | when matched then delete
           |""".stripMargin)(failedToResolveErrorMessage)

      //
      // 2.b) set source column name to be different with target column
      //
      spark.sql(
        s"""
           | merge into $tableName1 t0
           | using (
           |  select 3 as s_id, 'a3' as s_name, 30 as s_price, 3000 as v, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id
           | when matched then delete
         """.stripMargin
      )

      checkAnswer(s"select id,name,price,v,dt from $tableName1 order by id")(
        Seq(1, "a1", 10, 1000, "2021-03-21")
      )
    })
  }

  test("Test MergeInto with combination of delete update insert") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val sourceTable = generateTableName
      val targetTable = generateTableName
      // Create source table
      spark.sql(
        s"""
           | create table $sourceTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           | ) using parquet
           | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
      spark.sql(s"insert into $sourceTable values(8, 's8', 80, 2008, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(9, 's9', 90, 2009, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(10, 's10', 100, 2010, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(11, 's11', 110, 2011, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(12, 's12', 120, 2012, '2021-03-21')")
      // Create target table
      spark.sql(
        s"""
           |create table $targetTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}/$targetTable'
       """.stripMargin)
      spark.sql(s"insert into $targetTable values(7, 'a7', 70, 1007, '2021-03-21')")
      spark.sql(s"insert into $targetTable values(8, 'a8', 80, 1008, '2021-03-21')")
      spark.sql(s"insert into $targetTable values(9, 'a9', 90, 1009, '2021-03-21')")
      spark.sql(s"insert into $targetTable values(10, 'a10', 100, 1010, '2021-03-21')")

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using $sourceTable as s0
           | on t0.id = s0.id
           | when matched and s0.id = 10 then delete
           | when matched and s0.id < 10 then update set id=s0.id, name='sxx', price=s0.price*2, ts=s0.ts+10000, dt=s0.dt
           | when not matched and s0.id > 10 then insert *
         """.stripMargin)
      checkAnswer(s"select id,name,price,ts,dt from $targetTable order by id")(
        Seq(7, "a7", 70, 1007, "2021-03-21"),
        Seq(8, "sxx", 160, 12008, "2021-03-21"),
        Seq(9, "sxx", 180, 12009, "2021-03-21"),
        Seq(11, "s11", 110, 2011, "2021-03-21"),
        Seq(12, "s12", 120, 2012, "2021-03-21")
      )
    })
  }

  test("Merge Hudi to Hudi") {
    withRecordType()(withTempDir { tmp =>
      spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      Seq("cow", "mor").foreach { tableType =>
        val sourceTable = generateTableName
        spark.sql(
          s"""
             |create table $sourceTable (
             | id int,
             | name string,
             | price double,
             | _ts long
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '${tmp.getCanonicalPath}/$sourceTable'
          """.stripMargin)

        val targetTable = generateTableName
        val targetBasePath = s"${tmp.getCanonicalPath}/$targetTable"
        spark.sql(
          s"""
             |create table $targetTable (
             | id int,
             | name string,
             | price double,
             | _ts long
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$targetBasePath'
          """.stripMargin)

        // First merge
        spark.sql(s"insert into $sourceTable values(1, 'a1', 10, 1000)")
        spark.sql(
          s"""
             |merge into $targetTable t0
             |using $sourceTable s0
             |on t0.id = s0.id
             |when not matched then insert *
          """.stripMargin)

        checkAnswer(s"select id, name, price, _ts from $targetTable")(
          Seq(1, "a1", 10, 1000)
        )
        val fs = FSUtils.getFs(targetBasePath, spark.sessionState.newHadoopConf())
        val firstCommitTime = HoodieDataSourceHelpers.latestCommit(fs, targetBasePath)

        // Second merge
        spark.sql(s"update $sourceTable set price = 12, _ts = 1001 where id = 1")
        spark.sql(
          s"""
             |merge into $targetTable t0
             |using $sourceTable s0
             |on t0.id = s0.id
             |when matched and cast(s0._ts as string) > '1000' then update set *
           """.stripMargin)
        checkAnswer(s"select id, name, price, _ts from $targetTable")(
          Seq(1, "a1", 12, 1001)
        )
        // Test incremental query
        val hudiIncDF1 = spark.read.format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
          .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommitTime)
          .load(targetBasePath)
        hudiIncDF1.createOrReplaceTempView("inc1")
        checkAnswer(s"select id, name, price, _ts from inc1")(
          Seq(1, "a1", 10, 1000)
        )
        val secondCommitTime = HoodieDataSourceHelpers.latestCommit(fs, targetBasePath)
        // Third merge
        spark.sql(s"insert into $sourceTable values(2, 'a2', 10, 1001)")
        spark.sql(
          s"""
             |merge into $targetTable t0
             |using $sourceTable s0
             |on t0.id = s0.id
             |when matched then update set *
             |when not matched and s0.name = 'a2' then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, _ts from $targetTable order by id")(
          Seq(1, "a1", 12, 1001),
          Seq(2, "a2", 10, 1001)
        )
        // Test incremental query
        val hudiIncDF2 = spark.read.format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, secondCommitTime)
          .load(targetBasePath)
        hudiIncDF2.createOrReplaceTempView("inc2")
        checkAnswer(s"select id, name, price, _ts from inc2 order by id")(
          Seq(1, "a1", 12, 1001),
          Seq(2, "a2", 10, 1001)
        )
      }
    })
  }

  test("Test Different Type of PreCombineField") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val typeAndValue = Seq(
        ("string", "'1000'"),
        ("int", 1000),
        ("bigint", 10000),
        ("timestamp", "'2021-05-20 00:00:00'"),
        ("date", "'2021-05-20'")
      )
      typeAndValue.foreach { case (dataType, dataValue) =>
        val tableName = generateTableName
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  c $dataType
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'c'
             | )
       """.stripMargin)

        // First merge with a extra input field 'flag' (insert a new record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, cast($dataValue as $dataType) as c, '1' as flag
             | ) s0
             | on s0.id = $tableName.id
             | when matched and flag = '1' then update set *
             | when not matched and flag = '1' then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(1, "a1", 10.0)
        )
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, cast($dataValue as $dataType) as c
             | ) s0
             | on s0.id = $tableName.id
             | when matched then update set
             | id = s0.id, name = s0.name, price = s0.price + $tableName.price, c = s0.c
             | when not matched then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(1, "a1", 20.0)
        )
      }
    })
  }

  test("Test MergeInto For MOR With Compaction On") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.compact.inline = 'true'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 10.0, 1000),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4",10.0, 1000)
      )

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 4 as id, 'a4' as name, 11 as price, 1000 as ts
           | ) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)

      // 5 commits will trigger compaction.
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 10.0, 1000),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4", 11.0, 1000)
      )
    })
  }

  test("Test MereInto With Null Fields") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val types = Seq(
        "string" ,
        "int",
        "bigint",
        "double",
        "float",
        "timestamp",
        "date",
        "decimal"
      )
      types.foreach { dataType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  value $dataType,
             |  ts int
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        spark.sql(
          s"""
             |merge into $tableName h0
             |using (
             | select 1 as id, 'a1' as name, cast(null as $dataType) as value, 1000 as ts
             | ) s0
             | on h0.id = s0.id
             | when not matched then insert *
             |""".stripMargin)
        checkAnswer(s"select id, name, value, ts from $tableName")(
          Seq(1, "a1", null, 1000)
        )
      }
    })
  }

  test("Test MergeInto With All Kinds Of DataType") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val dataAndTypes = Seq(
        ("string", "'a1'"),
        ("int", "10"),
        ("bigint", "10"),
        ("double", "10.0"),
        ("float", "10.0"),
        ("decimal(5,2)", "10.11"),
        ("decimal(5,0)", "10"),
        ("timestamp", "'2021-05-20 00:00:00'"),
        ("date", "'2021-05-20'")
      )
      dataAndTypes.foreach { case (dataType, dataValue) =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  value $dataType,
             |  ts int
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        spark.sql(
          s"""
             |merge into $tableName h0
             |using (
             | select 1 as id, 'a1' as name, cast($dataValue as $dataType) as value, 1000 as ts
             | ) s0
             | on h0.id = s0.id
             | when not matched then insert *
             |""".stripMargin)
        checkAnswer(s"select id, name, cast(value as string), ts from $tableName")(
          Seq(1, "a1", extractRawValue(dataValue), 1000)
        )
      }
    })
  }

  test("Test MergeInto with no-full fields source") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  value int,
           |  ts int
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 1 as id, 1001 as ts
           | ) s0
           | on h0.id = s0.id
           | when matched then update set h0.ts = s0.ts
           |""".stripMargin)
      checkAnswer(s"select id, name, value, ts from $tableName")(
        Seq(1, "a1", 10, 1001)
      )
    })
  }

  test("Test Merge Into with target matched columns cast-ed") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  value int,
           |  ts int
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      // Can remove redundant symmetrical casting on both sides (should succeed)
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select cast(1 as int) as id, 1004 as ts
           | ) s0
           | on cast(h0.id as string) = cast(s0.id as string)
           | when matched then update set h0.ts = s0.ts
           |""".stripMargin)

      checkAnswer(s"select id, name, value, ts from $tableName")(
        Seq(1, "a1", 10, 1004)
      )
    })
  }

  test("Test MergeInto with partial insert") {
    Seq(true, false).foreach { sparkSqlOptimizedWrites =>
      withRecordType()(withTempDir { tmp =>
        spark.sql("set hoodie.payload.combined.schema.validate = true")
        // Create a partitioned mor table
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}'
         """.stripMargin)

        spark.sql(s"insert into $tableName select 1, 'a1', 10, '2021-03-21'")

        // test with optimized sql merge enabled / disabled.
        spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=$sparkSqlOptimizedWrites")

        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 2 as id, 'a2' as name, 10 as price, '2021-03-20' as dt
             | ) s0
             | on s0.id = t0.id
             | when not matched and s0.id % 2 = 0 then insert (id, name, dt)
             | values(s0.id, s0.name, s0.dt)
         """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1", 10, "2021-03-21"),
          Seq(2, "a2", null, "2021-03-20")
        )
      })
    }
  }

  test("Test MergeInto with payloads default configuration - partitioned and non-partitioned with different small file group limits") {
    Seq(0, 1).foreach { smallFileGroupLimit =>
      withSQLConf("hoodie.merge.small.file.group.candidates.limit" -> smallFileGroupLimit.toString) {
        withTempDir { tmp =>
          Seq(true, false).foreach { isPartitioned =>
            Seq("cow", "mor").foreach { tableType =>
              val sourceTable = generateTableName
              spark.sql(
                s"""
                   |CREATE TABLE $sourceTable (
                   |    id INT,
                   |    name STRING,
                   |    price INT,
                   |    timestamp BIGINT,
                   |    category STRING
                   |) USING hudi
                   |LOCATION '${tmp.getCanonicalPath}/$sourceTable'
                   |""".stripMargin)

              spark.sql(
                s"""
                   |INSERT INTO $sourceTable
                   |VALUES (1, 'John Doe Update Failed', 19, 1, 'A'),
                   |       (3, 'Bob Smith Update Succeeded With a larger Precombine Field', 49, 1599058800, 'A'),
                   |       (4, 'Alice Johnson New Insert', 49, 2, 'B'),
                   |       (5, 'Baker Mayfield Updated given Same precombine Key value', 6, 10, 'A')
                   |""".stripMargin)

              val targetTable = generateTableName
              val partitionClause = if (isPartitioned) "PARTITIONED BY (category)" else ""

              spark.sql(
                s"""
                   |CREATE TABLE $targetTable (
                   |  id INT,
                   |  name STRING,
                   |  price INT,
                   |  timestamp BIGINT,
                   |  category STRING
                   |) USING hudi
                   |$partitionClause
                   |TBLPROPERTIES (
                   |  type = '$tableType',
                   |  primaryKey = 'id',
                   |  preCombineField = 'timestamp'
                   |)
                   |LOCATION '${tmp.getCanonicalPath}/$targetTable'
                   |""".stripMargin)

              val insertPartitionClause = if (isPartitioned) "partition(category)" else ""
              spark.sql(
                s"""
                   |INSERT INTO $targetTable $insertPartitionClause
                   |VALUES (1, 'John Doe Existing larger precombine key prevails', 19, 1598886001, 'A'),
                   |       (2, 'Jane Doe', 24, 1598972400, 'B'),
                   |       (3, 'Bob Smith', 14, 1, 'A'),
                   |       (5, 'Baker Mayfield', 60, 10, 'A')
                   |""".stripMargin)

              spark.sql(
                s"""
                   |MERGE INTO $targetTable t
                   |USING $sourceTable s
                   |ON t.price = s.price AND t.category = s.category
                   |WHEN MATCHED THEN UPDATE SET
                   |    t.id = s.id,
                   |    t.name = s.name,
                   |    t.price = s.price,
                   |    t.timestamp = s.timestamp,
                   |    t.category = s.category
                   |WHEN NOT MATCHED THEN INSERT
                   |    (id, name, price, timestamp, category)
                   |VALUES
                   |    (s.id, s.name, s.price, s.timestamp, s.category)
                   |""".stripMargin)

              checkAnswer(s"SELECT id, name, price, timestamp, category FROM $targetTable ORDER BY id, category")(
                  Seq(1, "John Doe Existing larger precombine key prevails", 19, 1598886001L, "A"),
                  Seq(2, "Jane Doe", 24, 1598972400L, "B"),
                  Seq(3, "Bob Smith Update Succeeded With a larger Precombine Field", 49, 1599058800L, "A"),
                  Seq(4, "Alice Johnson New Insert", 49, 2L, "B"),
                  Seq(5, "Baker Mayfield Updated given Same precombine Key value", 6, 10L, "A")
              )

              // Clean up
              spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
              spark.sql(s"DROP TABLE IF EXISTS $targetTable")
            }
          }
        }
      }
    }
  }
}
