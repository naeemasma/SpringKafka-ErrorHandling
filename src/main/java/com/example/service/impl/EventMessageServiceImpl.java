package com.example.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.example.domain.EventMessage;
import com.example.service.EventMessageService;

@Service("EventMessageService")
public class EventMessageServiceImpl implements EventMessageService {
	private final Logger logger = LoggerFactory.getLogger(EventMessageServiceImpl.class);
	
	@Override
	public void process(EventMessage eventMessage) throws Exception{
		if(eventMessage.getDescription().toUpperCase().startsWith("ERROR"))
			throw new RuntimeException("Failed message processing");
	}

}
