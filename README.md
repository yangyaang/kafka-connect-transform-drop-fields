# kafka-connect-transform-drop-fields

drop field by its value, only support string value now

example : drop all fields have value equal '__debezium_unavailable_value' which is created by debezium postgresql connector 
https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-toasted-values

usage:  
"transforms": "DropFields",  
"transforms.DropFields.type": "io.yangyaang.kafka.connect.transform.DropFields$Value",  
"transforms.DropFields.field.value": "__debezium_unavailable_value"  

Custom transformations see: https://docs.confluent.io/cloud/current/connectors/transforms/custom.html
