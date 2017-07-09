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

package com.main.wordcounter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import io.netty.handler.codec.string.StringDecoder;
import scala.Tuple2;
/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 *   `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
 *    zoo03 my-consumer-group topic1,topic2 1`
 */

public final class JavaKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  private JavaKafkaWordCount() {
  }

  public static void main(String[] args) throws Exception {
	args= new String[4];
	args[0] ="hdp-3.tap-psnc.net:2181";
	args[1] = "mygroup";
	args[2] = "kafka-test";
	args[3] = "1";
	
    if (args.length < 4) {
      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }

    //SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount").setMaster("spark://osboxes:7077");;
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount").setMaster("local[2]");
    // Create the context with 2 seconds batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<>();
    String[] topics = args[2].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }
    
    /* Not direct input stream
    JavaPairReceiverInputDStream<String, String> messages =
    		org.apache.spark.streaming.kafka.KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
    */
    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", "hdp-2.tap-psnc.net:6667");
    kafkaParams.put("bootstrap.servers", "hdp-2.tap-psnc.net:6667");
    kafkaParams.put("group.id", "mygroup");
    kafkaParams.put("zookeeper.connect", "hdp-3.tap-psnc.net:2181");
   
    
    Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics[0]));
    
    JavaPairInputDStream<String, String> directKafkaStream =
    		//(JavaStreamingContext, Class<K>, Class<V>, Class<KD>, Class<VD>, Map<String,String>, Set<String>)
    		org.apache.spark.streaming.kafka.KafkaUtils.createDirectStream(jssc, String.class, String.class, kafka.serializer.StringDecoder.class,kafka.serializer.StringDecoder.class, kafkaParams, topicsSet);
    		
    		//createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
    		
    JavaDStream<String> lines = directKafkaStream.map(Tuple2::_2);
    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
        .reduceByKey((i1, i2) -> i1 + i2);

    //wordCounts.
    JavaDStream<Long> countedWords = wordCounts.count();
    countedWords.print();
    wordCounts.print();
    jssc.start();
    jssc.awaitTermination();
  }
}