/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.container.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.DefaultRetryState;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Dave Syer
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/org/springframework/batch/jms/jms-context.xml")
public class BatchMessageListenerContainerIntegrationTests {

	private static final Log logger = LogFactory.getLog(BatchMessageListenerContainerIntegrationTests.class);

	@Autowired
	private JmsTemplate jmsTemplate;

	@Autowired
	private BatchMessageListenerContainer container;

	@Autowired
	private Destination forward;

	private volatile BlockingQueue<String> recovered = new LinkedBlockingQueue<String>();

	private volatile BlockingQueue<String> processed = new LinkedBlockingQueue<String>();

	@After
	@Before
	public void drainQueue() throws Exception {
		container.stop();
		jmsTemplate.setReceiveTimeout(JmsTemplate.RECEIVE_TIMEOUT_NO_WAIT);
		while (jmsTemplate.receiveAndConvert("queue") != null) {
			// do nothing
		}
		while (jmsTemplate.receiveAndConvert("forward") != null) {
			// do nothing
		}
		processed.clear();
	}

	@AfterClass
	public static void giveContainerTimeToStop() throws Exception {
		Thread.sleep(1000);
	}

	@Test
	public void testConfiguration() throws Exception {
		assertNotNull(container);
	}

	@Test
	public void testSendAndReceive() throws Exception {
		final AtomicInteger redeliveries = new AtomicInteger();
		container.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message msg) {
				try {
					try {
						String text = ((TextMessage) msg).getText();
						boolean redelivered = msg.getJMSRedelivered();
						if (redelivered) {
							redeliveries.incrementAndGet();
						}
						logger.info("Delivered:" + text + " redelivered:" + redelivered);
						Thread.sleep(10);
						jmsTemplate.convertAndSend(forward, msg);
						processed.add(text);
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				catch (JMSException e) {
					throw new IllegalStateException(e);
				}
			}
		});
		jmsTemplate.setExplicitQosEnabled(true);
		jmsTemplate.setDeliveryPersistent(true); // bug? this is supposed to be default
		jmsTemplate.setSessionTransacted(false); // speed up sends
		for (int i = 0; i < 1000; i++) {
			try {
				jmsTemplate.convertAndSend("queue", "foo" + i);
				jmsTemplate.convertAndSend("queue", "bar" + i);
			}
			catch (Exception e) {
				logger.info(e.getMessage());
				Thread.sleep(1000);
			}
		}
		jmsTemplate.setSessionTransacted(true);
		container.start();
		logger.info("sent");
		jmsTemplate.setReceiveTimeout(10000);
		SortedSet<String> result = new TreeSet<String>();
		int forwards = 0;
		int received = 0;
		while(received < 2000) {
			String poll = processed.poll(10, TimeUnit.SECONDS);
			if (poll != null) {
				result.add(poll);
				logger.info("rcv:" + poll + " " + ++received);
				try{
		 			Object receive = jmsTemplate.receiveAndConvert(forward);
		 			if (receive != null) {
		 				logger.info("fwd:" + receive + " " + ++forwards);
		 			}
				}
				catch (Exception e) {
					System.out.println(e);
				}
			}
		}
		while(forwards < 2000) {
			try{
	 			Object receive = jmsTemplate.receiveAndConvert(forward);
	 			if (receive != null) {
	 				logger.info("fwd:" + receive + " " + ++forwards);
	 			}
			}
			catch (Exception e) {
				System.out.println(e);
			}
		}
		logger.info("Done; redeliveries:" + redeliveries + ", received: " + received + ", forwarded:" + forwards);
	}

	@Test @Ignore
	public void testFailureAndRepresent() throws Exception {
		container.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message msg) {
				try {
					processed.add(((TextMessage) msg).getText());
				}
				catch (JMSException e) {
					throw new IllegalStateException(e);
				}
				throw new RuntimeException("planned failure for represent: " + msg);
			}
		});
		container.initializeProxy();
		container.start();
		jmsTemplate.convertAndSend("queue", "foo");
		for (int i = 0; i < 2; i++) {
			assertEquals("foo", processed.poll(5, TimeUnit.SECONDS));
		}
	}

	@Test @Ignore
	public void testFailureAndRecovery() throws Exception {
		final RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(new NeverRetryPolicy());
		container.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(final Message msg) {
				try {
					RetryCallback<Message, Exception> callback = new RetryCallback<Message, Exception>() {
						@Override
						public Message doWithRetry(RetryContext context) throws Exception {
							try {
								processed.add(((TextMessage) msg).getText());
							}
							catch (JMSException e) {
								throw new IllegalStateException(e);
							}
							throw new RuntimeException("planned failure: " + msg);
						}
					};
					RecoveryCallback<Message> recoveryCallback = new RecoveryCallback<Message>() {
						@Override
						public Message recover(RetryContext context) {
							try {
								recovered.add(((TextMessage) msg).getText());
							}
							catch (JMSException e) {
								throw new IllegalStateException(e);
							}
							return msg;
						}
					};
					retryTemplate.execute(callback, recoveryCallback, new DefaultRetryState(msg.getJMSMessageID()));
				}
				catch (Exception e) {
					throw (RuntimeException) e;
				}
			}
		});
		container.initializeProxy();
		container.start();
		jmsTemplate.convertAndSend("queue", "foo");
		assertEquals("foo", processed.poll(5, TimeUnit.SECONDS));
		assertEquals("foo", recovered.poll(5, TimeUnit.SECONDS));
	}

}
