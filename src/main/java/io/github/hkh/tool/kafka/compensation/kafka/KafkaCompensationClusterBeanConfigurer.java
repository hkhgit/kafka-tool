package io.github.hkh.tool.kafka.compensation.kafka;

import com.google.common.collect.Maps;
import io.github.hkh.tool.kafka.config.compensation.CompensationProperties;
import io.github.hkh.tool.kafka.config.compensation.KafkaTypeCompensationProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import io.github.hkh.tool.kafka.compensation.scheduler.KafkaCompensationWakeUpScheduler;
import io.github.hkh.tool.kafka.sender.ReliableSender;
import io.github.hkh.tool.kafka.utils.Binder.BinderUtil;
import io.github.hkh.tool.kafka.utils.SpringContextUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Description: kafka补偿方式-集群配置器
 *
 * @author kai
 * @date 2025/1/3
 */
@Slf4j
public class KafkaCompensationClusterBeanConfigurer  implements BeanDefinitionRegistryPostProcessor, PriorityOrdered, ApplicationListener<ContextRefreshedEvent> {

    private static final String KAFKA_TOOL_COMPENSATE_CONFIG_PREFIX = "kafka-tool.compensate";

    private static final String DEFAULT_COMPENSATE_CONSUMER_FACTORY_NAME = "AutoCompensateConsumerFactory";

    private static final String DEFAULT_COMPENSATE_CONSUMER_CONTAINER_FACTORY_NAME = "AutoCompensateListenerContainerFactory";

    private static final String DEFAULT_COMPENSATE_CONSUMER_NAME = "AutoCompensateConsumer";

    private static final String DEFAULT_COMPENSATE_KAFKA_TEMPLATE_NAME = "AutoCompensateKafkaTemplate";

    private static final String DEFAULT_COMPENSATE_RETRY_PRODUCER = "AutoRetryProducer";

    private static final String DEFAULT_COMPENSATE_KAFKA_SCHEDULER = "AutoCompensateScheduler";

    private final CompensationProperties compensationProperties;

    public KafkaCompensationClusterBeanConfigurer(Environment environment) {
        BinderUtil.initBinder(environment);
        this.compensationProperties = BinderUtil.bind(CompensationProperties.class, KAFKA_TOOL_COMPENSATE_CONFIG_PREFIX);
    }

    private String getDynamicKafkaTemplateName(String clusterName){
        return clusterName + DEFAULT_COMPENSATE_KAFKA_TEMPLATE_NAME;
    }

    public static String getDynamicCompensateProducerBeanName(String clusterName){
        return clusterName + DEFAULT_COMPENSATE_RETRY_PRODUCER;
    }

    public static String getCompensateListenerBeanName(String clusterName) {
        return clusterName + DEFAULT_COMPENSATE_CONSUMER_NAME;
    }


    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        if (Objects.isNull(compensationProperties) || MapUtils.isEmpty(compensationProperties.getClusters())){
            return;
        }
        // 逐个处理集群的配置
        compensationProperties.getClusters().forEach((clusterName, cluster) -> {
            // 注入kafkaListenerContainerFactory
//            registerKafkaListenerContainerFactory(registry, clusterName, cluster);
            // 注入ProducerFactory
//            registerAutoRetryCompensateProducer(registry, clusterName, cluster);
            // 注入kafkaTemplate
            registerKafkaTemplate(registry, clusterName, cluster);
        });

    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        //监听Spring容器启动完成事件开始注册补偿相关的bean
        if (Objects.isNull(compensationProperties) || MapUtils.isEmpty(compensationProperties.getClusters())){
            log.info("<kafka-tool>未配置补偿相关配置，无需注册补偿相关bean");
            return ;
        }
        AutowireCapableBeanFactory autowireCapableBeanFactory = event.getApplicationContext().getAutowireCapableBeanFactory();
//        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry = event.getApplicationContext().getBean(KafkaListenerEndpointRegistry.class);
        ReliableSender reliableSender = event.getApplicationContext().getBean(ReliableSender.class);

        // 逐个处理集群的配置
        compensationProperties.getClusters().forEach((clusterName, cluster) -> {
            //补偿生产者需要独立加载，避免关闭补偿开关导致补偿生产者也关闭了，导致补偿消息无法发送
            registerAutoRetryCompensateProducer((DefaultListableBeanFactory) autowireCapableBeanFactory, clusterName, cluster, reliableSender );
            if (compensationProperties.enabled){
                // 注入补偿消费者唤醒定时器
                registerAutoRetryScheduler((DefaultListableBeanFactory) autowireCapableBeanFactory, clusterName);
                // 注入补偿消费者
                registerListeners((DefaultListableBeanFactory) autowireCapableBeanFactory, clusterName, compensationProperties);
            }
        });
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private void registerKafkaTemplate(BeanDefinitionRegistry registry, String clusterName, KafkaTypeCompensationProperties kafkaTypeCompensateProperties) {
        BeanDefinitionBuilder kafkaTemplateBuilder = BeanDefinitionBuilder.genericBeanDefinition(KafkaTemplate.class,
                () -> createKafkaTemplate(kafkaTypeCompensateProperties));
        String kafkaTemplateBeanName = getDynamicKafkaTemplateName(clusterName);
        registry.registerBeanDefinition(kafkaTemplateBeanName, kafkaTemplateBuilder.getBeanDefinition());
    }

    private KafkaTemplate<String, String> createKafkaTemplate(KafkaTypeCompensationProperties kafkaTypeCompensationProperties) {
        Map<String, Object> stringObjectMap = new HashMap<>(getBootstrapServerMap(kafkaTypeCompensationProperties));

        stringObjectMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        stringObjectMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        stringObjectMap.put(ProducerConfig.ACKS_CONFIG, "1");

        DefaultKafkaProducerFactory<String, String> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(stringObjectMap);
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    private Map<String, String> getBootstrapServerMap(KafkaTypeCompensationProperties kafkaTypeCompensateProperties){
        Map<String, String> bootStrapServerMap = Maps.newHashMap();
        bootStrapServerMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTypeCompensateProperties.getBootstrapServers());
        return bootStrapServerMap;
    }

    private Map<String, Object> buildConsumerProperties(KafkaTypeCompensationProperties kafkaTypeCompensateProperties) {
        Map<String, Object> props = new HashMap<String, Object>(16);

        props.putAll(getBootstrapServerMap(kafkaTypeCompensateProperties));
        // 把auto.commit.offset设为false，让应用程序决定何时提交偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, compensationProperties.getSchedulerCompensationMaxLimit());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaTypeCompensateProperties.getMaxPollIntervalMs());
        String groupId = kafkaTypeCompensateProperties.getGroupId();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }



    private void registerAutoRetryCompensateProducer(DefaultListableBeanFactory beanFactory, String clusterName, KafkaTypeCompensationProperties kafkaTypeCompensateProperties, ReliableSender reliableSender) {
        String kafkaTemplateBeanName = getDynamicKafkaTemplateName(clusterName);
        BeanDefinitionBuilder autoRetryCompensateProducerBuilder = BeanDefinitionBuilder.genericBeanDefinition(KafkaCompensationProducer.class,
                () -> new KafkaCompensationProducer(beanFactory.getBean(kafkaTemplateBeanName, KafkaTemplate.class),
                        kafkaTemplateBeanName, kafkaTypeCompensateProperties, reliableSender));
        String autoRetryCompensateProducerBeanName = getDynamicCompensateProducerBeanName(clusterName);
        beanFactory.registerBeanDefinition(autoRetryCompensateProducerBeanName, autoRetryCompensateProducerBuilder.getBeanDefinition());
    }

    private void registerAutoRetryScheduler(DefaultListableBeanFactory beanFactory, String clusterName) {
        BeanDefinitionBuilder autoRetryCompensateProducerBuilder = BeanDefinitionBuilder.genericBeanDefinition(KafkaCompensationWakeUpScheduler.class,
                () -> new KafkaCompensationWakeUpScheduler(compensationProperties, clusterName));
        String autoRetryCompensateProducerBeanName = clusterName + DEFAULT_COMPENSATE_KAFKA_SCHEDULER;
        beanFactory.registerBeanDefinition(autoRetryCompensateProducerBeanName, autoRetryCompensateProducerBuilder.getBeanDefinition());
    }


    private ConsumerFactory<String, String> createAutoCompensateConsumerFactory(KafkaTypeCompensationProperties kafkaTypeCompensateProperties) {
        this.checkCompensateConfig(kafkaTypeCompensateProperties);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(buildConsumerProperties(kafkaTypeCompensateProperties));
        return consumerFactory;
    }

    private void checkCompensateConfig(KafkaTypeCompensationProperties compensateProperties) {
        String compensateTopic = compensateProperties.getTopic();
        if (StringUtils.isBlank(compensateTopic)) {
//            throw new ConfigurationRequiredException("<kafka-tool>Kafka自动补偿 Topic 未配置");
        }
    }

    private void registerListeners(DefaultListableBeanFactory beanFactory, String clusterName, CompensationProperties compensationProperties) {
        //创建 Kafka 消费者工厂
        KafkaTypeCompensationProperties kafkaTypeCompensationProperties = compensationProperties.getClusters().get(clusterName);
        ConsumerFactory<String, String> autoCompensateConsumerFactory = createAutoCompensateConsumerFactory(kafkaTypeCompensationProperties);

        // 配置 Kafka 消费者容器属性
        KafkaCompensationProducer bean = SpringContextUtil.getBean(getDynamicCompensateProducerBeanName(clusterName), KafkaCompensationProducer.class);
        ContainerProperties containerProps = new ContainerProperties(kafkaTypeCompensationProperties.getTopic());
        containerProps.setGroupId(kafkaTypeCompensationProperties.getGroupId());

        if (compensationProperties.getSchedulerCompensationMaxLimit() > 1) {
            BatchCompensationConsumer batchCompensationConsumer = new BatchCompensationConsumer(clusterName, bean, compensationProperties);
            containerProps.setMessageListener(batchCompensationConsumer);
            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
            log.info("<kafka-tool>注册自动补偿消费者 BatchCompensationConsumer");
        }else {
            SingleCompensationConsumer autoSingleRetryConsumer = new SingleCompensationConsumer(clusterName, bean, compensationProperties);
            containerProps.setMessageListener(autoSingleRetryConsumer);
            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            log.info("<kafka-tool>注册自动补偿消费者 SingleCompensationConsumer");
        }

        String beanName = getCompensateListenerBeanName(clusterName);
        // 创建 Kafka 消费者容器
        ConcurrentMessageListenerContainer<String, String> listenerContainer = new ConcurrentMessageListenerContainer<>(autoCompensateConsumerFactory, containerProps);
        listenerContainer.setAutoStartup(false);
        BeanDefinitionBuilder listenerBeanBuilder = BeanDefinitionBuilder.genericBeanDefinition(ConcurrentMessageListenerContainer.class,
                () -> listenerContainer);
        beanFactory.registerBeanDefinition(beanName, listenerBeanBuilder.getBeanDefinition());
    }



}
