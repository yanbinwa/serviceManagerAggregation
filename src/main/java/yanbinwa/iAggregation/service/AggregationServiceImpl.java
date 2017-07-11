package yanbinwa.iAggregation.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.common.kafka.consumer.IKafkaCallBack;
import yanbinwa.common.kafka.consumer.IKafkaConsumer;
import yanbinwa.common.kafka.message.KafkaMessage;
import yanbinwa.common.kafka.producer.IKafkaProducer;
import yanbinwa.common.orchestrationClient.OrchestartionCallBack;
import yanbinwa.common.orchestrationClient.OrchestrationClient;
import yanbinwa.common.orchestrationClient.OrchestrationClientImpl;
import yanbinwa.common.orchestrationClient.OrchestrationServiceState;
import yanbinwa.common.utils.KafkaUtil;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;

@Service("cacheService")
@EnableAutoConfiguration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "serviceProperties")
public class AggregationServiceImpl implements AggregationService
{
    
    private static final Logger logger = Logger.getLogger(AggregationServiceImpl.class);
    
    Map<String, String> serviceDataProperties;
    Map<String, String> zNodeInfoProperties;
    Map<String, Object> kafkaProperties;

    public void setServiceDataProperties(Map<String, String> properties)
    {
        this.serviceDataProperties = properties;
    }
    
    public Map<String, String> getServiceDataProperties()
    {
        return this.serviceDataProperties;
    }
    
    public void setZNodeInfoProperties(Map<String, String> properties)
    {
        this.zNodeInfoProperties = properties;
    }
    
    public Map<String, String> getZNodeInfoProperties()
    {
        return this.zNodeInfoProperties;
    }
    
    public void setKafkaProperties(Map<String, Object> properties)
    {
        this.kafkaProperties = properties;
    }
    
    public Map<String, Object> getKafkaProperties()
    {
        return this.kafkaProperties;
    }
    
    ZNodeServiceData serviceData = null;
    
    OrchestrationClient client = null;
    
    Map<String, IKafkaProducer> kafkaProducerMap = new HashMap<String, IKafkaProducer>();
    
    Map<String, IKafkaConsumer> kafkaConsumerMap = new HashMap<String, IKafkaConsumer>();
    
    boolean isRunning = false;
    
    OrchestrationWatcher watcher = new OrchestrationWatcher();
    
    IKafkaCallBack callback = new IKafkaCallBackImpl();
    
    @Override
    public void afterPropertiesSet() throws Exception
    {
        String zookeeperHostIp = zNodeInfoProperties.get(OrchestrationClient.ZOOKEEPER_HOSTPORT_KEY);
        if(zookeeperHostIp == null)
        {
            logger.error("Zookeeper host and port should not be null");
            return;
        }
        String serviceGroupName = serviceDataProperties.get(AggregationService.SERVICE_SERVICEGROUPNAME);
        String serviceName = serviceDataProperties.get(AggregationService.SERVICE_SERVICENAME);
        String ip = serviceDataProperties.get(AggregationService.SERVICE_IP);
        String portStr = serviceDataProperties.get(AggregationService.SERVICE_PORT);
        int port = Integer.parseInt(portStr);
        String rootUrl = serviceDataProperties.get(AggregationService.SERVICE_ROOTURL);
        String topicInfo = serviceDataProperties.get(AggregationService.SERVICE_TOPICINFO);
        serviceData = new ZNodeServiceDataImpl(ip, serviceGroupName, serviceName, port, rootUrl);
        serviceData.addServiceDataDecorate(ZNodeDecorateType.KAFKA, topicInfo);
        
        client = new OrchestrationClientImpl(serviceData, watcher, zookeeperHostIp, zNodeInfoProperties);
        createKafkaProducerAndConsumer(kafkaProperties);
        
        start();
    }
    
    @Override
    public void start()
    {
        if(!isRunning)
        {
            logger.info("Start cache service ...");
            client.start();
            isRunning = true;
        }
        else
        {
            logger.info("Cache service has already started ...");
        }
    }

    @Override
    public void stop()
    {
        if(isRunning)
        {
            logger.info("Stop cache service ...");
            client.stop();
            isRunning = false;
        }
        else
        {
            logger.info("Cache service has already stopped ...");
        }
    }

    @Override
    public String getServiceName() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return serviceData.getServiceName();
    }

    @Override
    public boolean isServiceReady() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return client.isReady();
    }

    @Override
    public String getServiceDependence() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return client.getDepData().toString();
    }

    @Override
    public void startManageService()
    {
        if(!isRunning)
        {
            start();
        }
    }

    @Override
    public void stopManageService()
    {
        if(isRunning)
        {
            stop();
        }
    }

    private void createKafkaProducerAndConsumer(Map<String, Object> kafkaProperties)
    {
        kafkaProducerMap = KafkaUtil.createKafkaProducerMap(kafkaProperties);
        kafkaConsumerMap = KafkaUtil.createKafkaConsumerMap(kafkaProperties, callback);
    }
    
    private void startKafkaProducers()
    {
        for(Map.Entry<String, IKafkaProducer> entry : kafkaProducerMap.entrySet())
        {
            entry.getValue().start();
        }
    }
    
    private void stopKafkaProducers()
    {
        for(Map.Entry<String, IKafkaProducer> entry : kafkaProducerMap.entrySet())
        {
            entry.getValue().stop();
        }
    }
    
    private void startKafkaConsumers()
    {
        for(Map.Entry<String, IKafkaConsumer> entry : kafkaConsumerMap.entrySet())
        {
            entry.getValue().start();
        }
    }
    
    private void stopKafkaConsumers()
    {
        for(Map.Entry<String, IKafkaConsumer> entry : kafkaConsumerMap.entrySet())
        {
            entry.getValue().stop();
        }
    }
    
    class IKafkaCallBackImpl implements IKafkaCallBack
    {

        @Override
        public void handleOnData(KafkaMessage msg)
        {
            logger.info("Get kafka message: " + msg);
        }
        
    }
    
    class OrchestrationWatcher implements OrchestartionCallBack
    {

        OrchestrationServiceState curState = OrchestrationServiceState.NOTREADY;
        
        @Override
        public void handleServiceStateChange(OrchestrationServiceState state)
        {
            logger.info("Service state is: " + state);
            //由Unready -> ready
            if (state == OrchestrationServiceState.READY && curState == OrchestrationServiceState.NOTREADY)
            {
                logger.info("The service is started");
                startKafkaProducers();
                startKafkaConsumers();
                curState = state;
            }
            else if(state == OrchestrationServiceState.NOTREADY && curState == OrchestrationServiceState.READY)
            {
                logger.info("The service is stopped");
                stopKafkaProducers();
                stopKafkaConsumers();
                curState = state;
            }
            else if(state == OrchestrationServiceState.DEPCHANGE)
            {
                logger.info("The dependence is changed");
            }
        }
    }
}
