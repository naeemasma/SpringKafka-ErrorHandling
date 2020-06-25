package com.example.service;

import com.example.domain.EventMessage;

public interface EventMessageService {

	public void process(EventMessage eventMessage) throws Exception;

}
