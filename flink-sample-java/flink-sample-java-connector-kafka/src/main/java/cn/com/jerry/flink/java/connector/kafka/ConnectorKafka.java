/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.com.jerry.flink.java.connector.kafka;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author GangW
 */
public class ConnectorKafka {
    private static final String SOURCE_TOPIC = "metric";
    private static final String SINK_TOPIC = "metric-sink";

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties sourceProps = new Properties();
        sourceProps.put("bootstrap.servers", "localhost:9092");
        sourceProps.put("group.id", "metric-group");
        // key 反序列化
        sourceProps.put("key.deserializer", StringDeserializer.class.getName());
        // value 反序列化
        sourceProps.put("value.deserializer", StringDeserializer.class.getName());
        sourceProps.put("auto.offset.reset", "latest");

        Properties sinkProps = new Properties();
        sinkProps.put("bootstrap.servers", "localhost:9092");
        sinkProps.put("group.id", "metric-group");
        sinkProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        sinkProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        sinkProps.put("auto.offset.reset", "latest");

        DataStreamSource<String> source =
            env.addSource(new FlinkKafkaConsumer011<>(SOURCE_TOPIC, new SimpleStringSchema(), sourceProps))
                .setParallelism(2);

        source.addSink(new FlinkKafkaProducer011<>(SINK_TOPIC, new SimpleStringSchema(), sinkProps)).setParallelism(2);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
