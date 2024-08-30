/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

/** Example Tests for {@link MySqlSource}. */
public class MySqlSourceExampleTest2 {

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("10.213.47.53")
                        .port(6606)
                        .databaseList("id_bke_data_warehouse_db")
                        .tableList("id_bke_data_warehouse_db" + ".wl_test_01")
                        .username("bank_test")
                        .password("asd!Qwe123")
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.history())
                        .serverTimeZone("UTC+8")
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        MySqlSource<String> mySqlSource2 =
                MySqlSource.<String>builder()
                        .hostname("10.213.47.53")
                        .port(6606)
                        .databaseList("id_bke_data_warehouse_db")
                        .tableList("id_bke_data_warehouse_db" + ".wl_test_02")
                        .username("bank_test")
                        .password("asd!Qwe123")
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.history())
                        .serverTimeZone("UTC+8")
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        config.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // enable checkpoint
        env.enableCheckpointing(3000);
        DataStreamSource<String> mySqlParallelSource1 =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                        .setParallelism(4);
        DataStreamSource<String> mySqlParallelSource2 =
                env.fromSource(
                                mySqlSource2,
                                WatermarkStrategy.noWatermarks(),
                                "MySqlParallelSource")
                        .setParallelism(4);
        DataStream<String> union = mySqlParallelSource1.union(mySqlParallelSource2);
        union.print().setParallelism(1);
        // set the source parallelism to 4
        env.execute("Print MySQL Snapshot + Binlog");
        //        JobClient jobClient = env.executeAsync("Print MySQL Snapshot + Binlog");
        //        Thread.sleep(30000);
        // 判断是否同步完，关闭任务

        //        jobClient.cancel();
    }
}
