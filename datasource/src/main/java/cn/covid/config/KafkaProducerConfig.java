package cn.covid.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;


import java.util.HashMap;
import java.util.Map;

/**
 * Author：
 * Date：2022/5/3119:59
 * Desc:KafkaTemplate配置类
 */

@Configuration      // 表示这是一个配置类，表示可以去application.properties读取配置
public class KafkaProducerConfig {
    @Value("${kafka.bootstrap.servers}")
    private String bootstrap_servers;
    @Value("${kafka.retries_config}")
    private String retries_config;
    @Value("${kafka.batch_size_config}")
    private String batch_size_config;
    @Value("${kafka.linger_ms_config}")
    private String linger_ms_config;
    @Value("${kafka.buffer_memory_config}")
    private String buffer_memory_config;

    @Bean //表示该方法返回对象交给Spring管理
    public KafkaTemplate kafkaTemplate(){
        Map<String,Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);
        configs.put(ProducerConfig.RETRIES_CONFIG,retries_config);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG,batch_size_config);
        configs.put(ProducerConfig.LINGER_MS_CONFIG,linger_ms_config);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG,buffer_memory_config);
        // 设置发送到kafka中的消息的Key/Value序列化类型，指定为<LocationId:Integer,Value:String>
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 指定自定义分区器
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class);
        DefaultKafkaProducerFactory producerFactory = new DefaultKafkaProducerFactory(configs);
        KafkaTemplate kafkaTemplate = new KafkaTemplate(producerFactory);
        return kafkaTemplate;
    }

}
