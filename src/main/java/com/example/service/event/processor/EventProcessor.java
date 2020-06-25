package com.example.service.event.processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.FixedBackOff;
import com.example.domain.EventMessage;
import com.example.domain.EventMessageTypeTwo;
import com.example.service.EventMessageService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Configuration
@EnableRetry
public class EventProcessor {
	
	@Value("${app.retry.attempts}") 
	private int retryAttempts;

	@Value("${app.retry.interval}") 
	private int retryInterval;
	
	@Value("${app.retry.topic}")
	private String retryTopic;
	
	@Value("${app.dlt.topic}")
	private String dltTopic;
	
	@Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
	
	@Autowired
    private EventMessageService eventMessageService;
			
    private final Logger logger = LoggerFactory.getLogger(EventProcessor.class);
        
	  @Bean 
	  public RecordMessageConverter converter() { 
		  return new StringJsonMessageConverter(); 
	  }
    	  
	  @Bean 
	  public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
		  ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
		  ConsumerFactory<Object, Object> kafkaConsumerFactory, 
		  KafkaTemplate<Object, Object> template
		  , ObjectMapper objectMapper
          ) 
	  { 
		  ConcurrentKafkaListenerContainerFactory<Object, Object>
		  factory = new ConcurrentKafkaListenerContainerFactory<>();
		  configurer.configure(factory, kafkaConsumerFactory);
		  factory.setErrorHandler(new SeekToCurrentErrorHandler((record, exception) -> {
		  try {
			    //Send message to the retry topic
				template.send(retryTopic, objectMapper.readValue(record.value().toString(), Object.class));
			} catch (JsonMappingException e) {
				logger.error(e.getMessage());
			} catch (JsonProcessingException e) {
				logger.error(e.getMessage());
			}
		        
		    }, new FixedBackOff(0, 0)));//A simple BackOff implementation with No retries.
		  return factory; 
	  }
	  
	  @Bean   
	  public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaRetryListenerContainerFactory(
		  ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
		  ConsumerFactory<Object, Object> kafkaConsumerFactory, 
		  KafkaTemplate<Object, Object> template
          ,ObjectMapper objectMapper
		  ) 
	  { 
		  ConcurrentKafkaListenerContainerFactory<Object, Object>
		  factory = new ConcurrentKafkaListenerContainerFactory<>();
		  configurer.configure(factory, kafkaConsumerFactory);
		  factory.setErrorHandler(new SeekToCurrentErrorHandler((record, exception) -> {
		  try {
				template.send(dltTopic, objectMapper.readValue(record.value().toString(), Object.class));
			} catch (JsonMappingException e) {
				logger.error(e.getMessage());
			} catch (JsonProcessingException e) {
				logger.error(e.getMessage());
			}
		        
		    }, new FixedBackOff(retryInterval, retryAttempts)));//A simple BackOff implementation that provides a configured interval between two attempts and a configured number of retries.
		  return factory; 
	  }
	  	
	@KafkaListener(topics = "#{'${app.consumer.subscribed-to.topic}'.split(',')}", containerFactory="kafkaListenerContainerFactory", groupId = "${app.consumer.group-id}")
	public void consume(EventMessage eventMessage) throws Exception {
		  try {
			    logger.info(String.format("Consumed Message: %s", eventMessage));
				//Process message
			    EventMessage msg= new EventMessage(eventMessage.getDescription()+"");
				eventMessageService.process(msg);
		  } catch (Exception e) {
				throw e;
		  }  
    }
    
    @KafkaListener(topics = "${app.dlt.topic}", containerFactory="kafkaListenerContainerFactory", groupId = "${app.consumer.group-id}")
    public void dltListen(Object eventMessage) {
      logger.info(String.format("Recieved Message in DLT: %s", eventMessage));
    }
    
    @KafkaListener(topics = "${app.retry.topic}", containerFactory="kafkaRetryListenerContainerFactory", groupId = "${app.consumer.group-id}")
    public void retry(EventMessage eventMessage) throws Exception {       
	    try {
	        logger.info(String.format("Recieved Message in retry: %s" , eventMessage)); 
			//Do something to re-process the message
	        EventMessage msg= new EventMessage(eventMessage.getDescription()+"");
			eventMessageService.process(msg);
		  } catch (Exception e) {
				throw e;
		  }  
    }
    
    public void sendEventMessage(String topic, String input) {
    		logger.info(String.format("Producing message: %s", input));
    		kafkaTemplate.send(topic,new EventMessageTypeTwo(input));
    }
}