package me.wonwoo;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonDeserializer implements Deserializer<Logs> {

	private ObjectMapper objectMapper;

	/**
	 * Default constructor needed by Kafka
	 */
	public KafkaJsonDeserializer() {

	}

	@Override
	public void configure(Map<String, ?> map, boolean b) {
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public Logs deserialize(String ignored, byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}

		try {
			return objectMapper.readValue(bytes, Logs.class);
		} catch (Exception e) {
			throw new SerializationException(e);
		}
	}


	@Override
	public void close() {

	}
}
