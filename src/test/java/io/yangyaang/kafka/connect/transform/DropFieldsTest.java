package io.yangyaang.kafka.connect.transform;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DropFieldsTest {
    private final DropFields<SinkRecord> dropFields = new DropFields.Value<>();

    @Test
    public void tombstoneSchemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.value", "abc, foo");

        dropFields.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = dropFields.apply(record);

        Assert.assertNull(transformedRecord.value());
        Assert.assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.value", "abc, foo");

        dropFields.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, null, 0);
        final SinkRecord transformedRecord = dropFields.apply(record);

        Assert.assertNull(transformedRecord.value());
        Assert.assertEquals(schema, transformedRecord.valueSchema());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void schemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.value", "abc, __dropFlag__");

        dropFields.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("dont", "__dropFlag__");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = dropFields.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        Assert.assertEquals(3, updatedValue.size());
        Assert.assertEquals(42, updatedValue.get("abc"));
        Assert.assertEquals(true, updatedValue.get("foo"));
        Assert.assertEquals("etc", updatedValue.get("etc"));
        Assert.assertNull( updatedValue.get("dont"));
    }

    @Test
    public void withSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.value", "abc, __dropFlag__");

        dropFields.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "__dropFlag__");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = dropFields.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();


        Assert.assertEquals(3, updatedValue.schema().fields().size());
        Assert.assertEquals(42, updatedValue.get("abc"));
        Assert.assertEquals(true, updatedValue.get("foo"));
        Assert.assertEquals("etc", updatedValue.get("etc"));
        Assert.assertNull( updatedValue.schema().field("dont"));
    }
}
