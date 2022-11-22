package io.yangyaang.kafka.connect.transform;

import io.yangyaang.kafka.connect.transform.util.NonEmptyListValidator;
import io.yangyaang.kafka.connect.transform.util.Requirements;
import io.yangyaang.kafka.connect.transform.util.SchemaUtil;
import io.yangyaang.kafka.connect.transform.util.SimpleConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public abstract class DropFields<R extends ConnectRecord<R>> implements Transformation<R> {

    private final Logger log = LoggerFactory.getLogger(DropFields.class);

    private static final String PURPOSE = "field drop";
    interface ConfigName {
        String fieldValue = "field.value";

    }
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.fieldValue, ConfigDef.Type.LIST, NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "Field value to drop , use commas to separate multiple values");

    private List<String> dropFieldValues;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        dropFieldValues = config.getList(ConfigName.fieldValue);
    }

    @Override
    public void close() {

    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }


    private R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMap(operatingValue(record), PURPOSE);
        value.entrySet().removeIf(entry -> entry.getValue() instanceof String && dropFieldValues.contains(entry.getValue()));

        return record;
    }

    private R applyWithSchema(R record) {
        final Struct value = Requirements.requireStruct(operatingValue(record), PURPOSE);
        Schema valueSchema = value.schema();

        List<Field> dropFields = new ArrayList<>();
        for (Field field : valueSchema.fields()){
            Object filedValue = value.get(field);
            if (filedValue instanceof String && dropFieldValues.contains(filedValue)){
                dropFields.add(field);
            }
        }

        if (dropFields.isEmpty()){
            return record;
        }

        Schema newSchema;

        final SchemaBuilder newSchemaBuilder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        for (Field field : valueSchema.fields()) {
            if (!dropFields.contains(field)) {
                newSchemaBuilder.field(field.name(), field.schema());
            }
        }
        newSchema = newSchemaBuilder.build();

        final Struct newValue = new Struct(newSchema);

        for (Field field : newSchema.fields()) {
            final Object fieldValue = value.get(field.name());
            newValue.put(field.name(), fieldValue);
        }
        log.info("drop field:{}, with value for record:{}", dropFields, value);
        return newRecord(record, newSchema, newValue);
    }
    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends DropFields<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends DropFields<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
