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

package org.apache.hudi.avro;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.SchemaBackwardsCompatibilityException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAvroSchemaUtils {

  private static final String SOURCE_SCHEMA = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"int\"]\n"
      + "    },\n"
      + "    {\n"
      + "        \"name\" : \"f1\",\n"
      + "        \"type\" : [ \"null\", {\n"
      + "           \"type\" : \"fixed\",\n"
      + "           \"name\" : \"f1\",\n"
      + "           \"namespace\" : \"\",\n"
      + "           \"size\" : 5,\n"
      + "           \"logicalType\" : \"decimal\",\n"
      + "           \"precision\" : 10,\n"
      + "           \"scale\" : 2\n"
      + "           }],\n"
      + "       \"default\" : null\n"
      + "      },\n"
      + "    {\n"
      + "      \"name\": \"nested_record\",\n"
      + "      \"type\": {\n"
      + "        \"name\": \"nested\",\n"
      + "        \"type\": \"record\",\n"
      + "        \"fields\": [\n"
      + "          {\n"
      + "            \"name\": \"string\",\n"
      + "            \"type\": [\"null\", \"string\"]\n"
      + "          },\n"
      + "          {\n"
      + "            \"name\": \"long\",\n"
      + "            \"type\": [\"null\", \"long\"]\n"
      + "          }\n"
      + "        ]\n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  private static final String PROJECTED_NESTED_SCHEMA_STRICT = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"int\"]\n"
      + "    },\n"
      + "    {\n"
      + "        \"name\" : \"f1\",\n"
      + "        \"type\" : [ \"null\", {\n"
      + "           \"type\" : \"fixed\",\n"
      + "           \"name\" : \"fixed\",\n"
      + "           \"namespace\" : \"example.schema.source.f1\",\n"
      + "           \"size\" : 5,\n"
      + "           \"logicalType\" : \"decimal\",\n"
      + "           \"precision\" : 10,\n"
      + "           \"scale\" : 2\n"
      + "           }],\n"
      + "       \"default\" : null\n"
      + "      },\n"
      + "    {\n"
      + "      \"name\": \"nested_record\",\n"
      + "      \"type\": {\n"
      + "        \"name\": \"nested\",\n"
      + "        \"type\": \"record\",\n"
      + "        \"fields\": [\n"
      + "          {\n"
      + "            \"name\": \"string\",\n"
      + "            \"type\": [\"null\", \"string\"]\n"
      + "          }\n"
      + "        ]\n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  private static final String PROJECTED_NESTED_SCHEMA_WITH_PROMOTION = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"long\"]\n"
      + "    },\n"
      + "    {\n"
      + "      \"name\": \"nested_record\",\n"
      + "      \"type\": {\n"
      + "        \"name\": \"nested\",\n"
      + "        \"type\": \"record\",\n"
      + "        \"fields\": [\n"
      + "          {\n"
      + "            \"name\": \"string\",\n"
      + "            \"type\": [\"null\", \"string\"]\n"
      + "          }\n"
      + "        ]  \n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_NullSchema() {
    // This test should throw an IllegalArgumentException
    assertThrows(IllegalArgumentException.class, () -> AvroSchemaUtils.createNewSchemaFromFieldsWithReference(null, Collections.emptyList()));
  }

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_NullObjectProps() {
    // Create a schema without any object properties
    String schemaStr = "{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [] }";
    Schema schema = new Schema.Parser().parse(schemaStr);

    // Ensure getObjectProps returns null by mocking or creating a schema without props
    Schema newSchema = AvroSchemaUtils.createNewSchemaFromFieldsWithReference(schema, Collections.emptyList());

    // Validate the new schema
    assertEquals("TestRecord", newSchema.getName());
    assertEquals(0, newSchema.getFields().size());
  }

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_WithObjectProps() {
    // Create a schema with object properties
    String schemaStr = "{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [], \"prop1\": \"value1\" }";
    Schema schema = new Schema.Parser().parse(schemaStr);

    // Add an object property to the schema
    schema.addProp("prop1", "value1");

    // Create new fields to add
    Schema.Field newField = new Schema.Field("newField", Schema.create(Schema.Type.STRING), null, (Object) null);
    Schema newSchema = AvroSchemaUtils.createNewSchemaFromFieldsWithReference(schema, Collections.singletonList(newField));

    // Validate the new schema
    assertEquals("TestRecord", newSchema.getName());
    assertEquals(1, newSchema.getFields().size());
    assertEquals("value1", newSchema.getProp("prop1"));
    assertEquals("newField", newSchema.getFields().get(0).name());
  }

  @Test
  public void testIsStrictProjection() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Schema projectedNestedSchema = new Schema.Parser().parse(PROJECTED_NESTED_SCHEMA_STRICT);

    // Case #1: Validate proper (nested) projected record schema

    assertTrue(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, sourceSchema));
    assertTrue(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(AvroSchemaUtils.isStrictProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createArray(sourceSchema),
            Schema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createMap(sourceSchema),
            Schema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createUnion(Schema.create(Schema.Type.NULL), sourceSchema),
            Schema.createUnion(Schema.create(Schema.Type.NULL), projectedNestedSchema)));
  }

  @Test
  public void testIsCompatibleProjection() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Schema projectedNestedSchema = new Schema.Parser().parse(PROJECTED_NESTED_SCHEMA_WITH_PROMOTION);

    // Case #1: Validate proper (nested) projected record schema (with promotion,
    //          number field promoted from int to long)

    assertTrue(AvroSchemaUtils.isCompatibleProjectionOf(sourceSchema, sourceSchema));
    assertTrue(AvroSchemaUtils.isCompatibleProjectionOf(sourceSchema, projectedNestedSchema));

    // NOTE: That [[isStrictProjectionOf]] should be false in that case
    assertFalse(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(AvroSchemaUtils.isCompatibleProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createArray(sourceSchema),
            Schema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createMap(sourceSchema),
            Schema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createUnion(Schema.create(Schema.Type.NULL), sourceSchema),
            Schema.createUnion(Schema.create(Schema.Type.NULL), projectedNestedSchema)));
  }

  private static final Schema FULL_SCHEMA = new Schema.Parser().parse("{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"record\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"a\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"b\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"c\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  } ]\n"
      + "}");

  private static final Schema SHORT_SCHEMA = new Schema.Parser().parse("{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"record\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"a\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"b\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  } ]\n"
      + "}\n");

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatibleProjectionNotAllowed(boolean shouldValidate) {
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, SHORT_SCHEMA, shouldValidate, false, Collections.emptySet()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatibleProjectionAllowed(boolean shouldValidate) {
    AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, SHORT_SCHEMA, shouldValidate, true, Collections.emptySet());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatiblePartitionDropCols(boolean shouldValidate) {
    AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, SHORT_SCHEMA, shouldValidate, false, Collections.singleton("c"));
  }

  private static final Schema BROKEN_SCHEMA = new Schema.Parser().parse("{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"broken\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"a\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"b\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"c\",\n"
      + "    \"type\" : [ \"null\", \"boolean\" ],\n"
      + "    \"default\" : null\n"
      + "  } ]\n"
      + "}");

  @Test
  public void  testBrokenSchema() {
    assertThrows(SchemaBackwardsCompatibilityException.class,
        () -> AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, BROKEN_SCHEMA, true, false, Collections.emptySet()));
  }

  @Test
  public void testAppendFieldsToSchemaDedupNested() {
    Schema fullSchema = new Schema.Parser().parse("{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"example.schema\",\n"
        + "  \"name\": \"source\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"number\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"nested_record\",\n"
        + "      \"type\": {\n"
        + "        \"name\": \"nested\",\n"
        + "        \"type\": \"record\",\n"
        + "        \"fields\": [\n"
        + "          {\n"
        + "            \"name\": \"string\",\n"
        + "            \"type\": [\"null\", \"string\"]\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"long\",\n"
        + "            \"type\": [\"null\", \"long\"]\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"other\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n");

    Schema missingFieldSchema = new Schema.Parser().parse("{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"example.schema\",\n"
        + "  \"name\": \"source\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"number\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"nested_record\",\n"
        + "      \"type\": {\n"
        + "        \"name\": \"nested\",\n"
        + "        \"type\": \"record\",\n"
        + "        \"fields\": [\n"
        + "          {\n"
        + "            \"name\": \"string\",\n"
        + "            \"type\": [\"null\", \"string\"]\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"other\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n");

    Option<Schema.Field> missingField = AvroSchemaUtils.findNestedField(fullSchema, "nested_record.long");
    assertTrue(missingField.isPresent());
    assertEquals(fullSchema, AvroSchemaUtils.appendFieldsToSchemaDedupNested(missingFieldSchema, Collections.singletonList(missingField.get())));
  }

  @Test
  public void testFindNestedFieldType() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Option<Schema.Type> field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "number");
    assertTrue(field.isPresent());
    assertEquals(Schema.Type.INT, field.get());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "nested_record.string");
    assertTrue(field.isPresent());
    assertEquals(Schema.Type.STRING, field.get());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "nested_record.long");
    assertTrue(field.isPresent());
    assertEquals(Schema.Type.LONG, field.get());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, null);
    assertTrue(field.isEmpty());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "");
    assertTrue(field.isEmpty());

    assertThrows(HoodieAvroSchemaException.class, () -> AvroSchemaUtils.findNestedFieldType(sourceSchema, "long"));
    assertThrows(HoodieAvroSchemaException.class, () -> AvroSchemaUtils.findNestedFieldType(sourceSchema, "nested_record.bool"));
    assertThrows(HoodieAvroSchemaException.class, () -> AvroSchemaUtils.findNestedFieldType(sourceSchema, "non_present_field.also_not_present"));
  }
}
