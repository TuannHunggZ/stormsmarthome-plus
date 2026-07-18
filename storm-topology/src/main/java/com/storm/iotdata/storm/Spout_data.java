package com.storm.iotdata.storm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.iotdata.models.StormConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Storm spout that subscribes to MQTT, parses incoming JSON messages, filters
 * only load events (property == 1), and emits tuples on the "data" stream.
 */
public class Spout_data extends BaseRichSpout {

	private static final Logger LOGGER = LoggerFactory.getLogger(Spout_data.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final String brokerUri;
	private final String topic;
	private final int qos;
	private final int maxEmitPerNextTuple;
	private final int queueCapacity;
	private final String streamIdData;
	private final String fieldId;
	private final String fieldTimestamp;
	private final String fieldValue;
	private final String fieldPlugId;
	private final String fieldHouseholdId;
	private final String fieldHouseId;
	private final int propertyLoad;
	private final int connectionTimeoutSeconds;

	private transient SpoutOutputCollector collector;
	private transient MqttClient mqttClient;
	private final BlockingQueue<LoadEvent> eventQueue;

	/**
	 * Creates a spout with the default broker URI and topic.
	 */
	public Spout_data() {
		this(new StormConfig());
	}

	/**
	 * Creates a spout from the loaded Storm configuration.
	 *
	 * @param stormConfig Loaded Storm configuration.
	 */
	public Spout_data(StormConfig stormConfig) {
		StormConfig.SpoutDataConfig spoutDataConfig = stormConfig.getSpoutDataConfig();
		this.brokerUri = spoutDataConfig.getBrokerUri();
		this.topic = spoutDataConfig.getBrokerTopic();
		this.qos = spoutDataConfig.getQos();
		this.maxEmitPerNextTuple = spoutDataConfig.getMaxEmitPerNextTuple();
		this.queueCapacity = spoutDataConfig.getQueueCapacity();
		this.streamIdData = spoutDataConfig.getStreamIdData();
		this.fieldId = spoutDataConfig.getFieldId();
		this.fieldTimestamp = spoutDataConfig.getFieldTimestamp();
		this.fieldValue = spoutDataConfig.getFieldValue();
		this.fieldPlugId = spoutDataConfig.getFieldPlugId();
		this.fieldHouseholdId = spoutDataConfig.getFieldHouseholdId();
		this.fieldHouseId = spoutDataConfig.getFieldHouseId();
		this.propertyLoad = spoutDataConfig.getPropertyLoad();
		this.connectionTimeoutSeconds = spoutDataConfig.getConnectionTimeoutSeconds();
		this.eventQueue = new LinkedBlockingQueue<>(this.queueCapacity);
	}

	/**
	 * Initializes the MQTT client, registers the callback, and subscribes to the configured topic.
	 *
	 * @param conf Storm configuration map.
	 * @param context Topology context.
	 * @param collector Storm spout collector used for emitting tuples.
	 */
	@Override
	public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		initializeMqttClient();
	}

	/**
	 * Drains a bounded number of parsed MQTT events from the queue and emits them to the data stream.
	 */
	@Override
	public void nextTuple() {
		int emitted = 0;

		while (emitted < maxEmitPerNextTuple) {
			LoadEvent event = eventQueue.poll();
			if (event == null) {
				break;
			}

			collector.emit(
				streamIdData,
				new Values(
					event.id,
					event.timestamp,
					event.value,
					event.plugId,
					event.householdId,
					event.houseId
				),
				event.id
			);
			emitted += 1;
		}

		if (emitted == 0) {
			Utils.sleep(1);
		}
	}

	/**
	 * Declares the output fields for the "data" stream.
	 *
	 * @param declarer Storm declarer.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(
			streamIdData,
			new Fields(
				fieldId,
				fieldTimestamp,
				fieldValue,
				fieldPlugId,
				fieldHouseholdId,
				fieldHouseId
			)
		);
	}

	/**
	 * Disconnects and closes the MQTT client.
	 */
	@Override
	public void close() {
		if (mqttClient == null) {
			return;
		}

		try {
			mqttClient.disconnect();
		} catch (MqttException exception) {
			LOGGER.warn("Failed to disconnect MQTT client cleanly", exception);
		} finally {
			try {
				mqttClient.close();
			} catch (MqttException exception) {
				LOGGER.warn("Failed to close MQTT client", exception);
			}
			mqttClient = null;
		}
	}

	/**
	 * Reliability is not required for this spout.
	 *
	 * @param msgId Storm message identifier.
	 */
	@Override
	public void ack(Object msgId) {
		// TODO: Reliability is not required for this spout.
	}

	/**
	 * Reliability is not required for this spout.
	 *
	 * @param msgId Storm message identifier.
	 */
	@Override
	public void fail(Object msgId) {
		// TODO: Reliability is not required for this spout.
	}

	private void initializeMqttClient() {
		try {
			String clientId = "storm-spout-" + UUID.randomUUID();
			mqttClient = new MqttClient(brokerUri, clientId);

			MqttConnectOptions connectOptions = new MqttConnectOptions();
			connectOptions.setAutomaticReconnect(true);
			connectOptions.setCleanSession(true);
			connectOptions.setConnectionTimeout(connectionTimeoutSeconds);

			mqttClient.setCallback(new MqttCallbackExtended() {
				@Override
				public void connectComplete(boolean reconnect, String serverURI) {
					if (reconnect) {
						LOGGER.info("MQTT reconnected to {}", serverURI);
					} else {
						LOGGER.info("MQTT connected to {}", serverURI);
					}

					subscribeToTopic();
				}

				@Override
				public void connectionLost(Throwable cause) {
					LOGGER.warn("MQTT connection lost", cause);
				}

				@Override
				public void messageArrived(String incomingTopic, MqttMessage message) {
					handleIncomingMessage(incomingTopic, message);
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {
					// Not used by this spout.
				}
			});

			mqttClient.connect(connectOptions);
		} catch (MqttException exception) {
			throw new IllegalStateException("Unable to initialize MQTT client", exception);
		}
	}

	private void subscribeToTopic() {
		if (mqttClient == null || !mqttClient.isConnected()) {
			return;
		}

		try {
			mqttClient.subscribe(topic, qos);
			LOGGER.info("Subscribed to MQTT topic {} with qos {}", topic, qos);
		} catch (MqttException exception) {
			LOGGER.error("Failed to subscribe to topic {}", topic, exception);
		}
	}

	private void handleIncomingMessage(String incomingTopic, MqttMessage message) {
		try {
			String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
			JsonNode root = OBJECT_MAPPER.readTree(payload);
			LoadEvent event = parseLoadEvent(root);

			if (event == null) {
				return;
			}

			boolean enqueued = eventQueue.offer(event);
			if (!enqueued) {
				LOGGER.warn("Event queue is full, dropping message from topic {}", incomingTopic);
			}
		} catch (Exception exception) {
			LOGGER.error("Failed to parse MQTT JSON payload from topic {}", incomingTopic, exception);
		}
	}

	private LoadEvent parseLoadEvent(JsonNode root) {
		if (root == null) {
			return null;
		}

		int property = getRequiredInt(root, "property");
		if (property != propertyLoad) {
			return null;
		}

		return new LoadEvent(
			getRequiredLong(root, "id"),
			getRequiredLong(root, "timestamp"),
			getRequiredDouble(root, "value"),
			getRequiredInt(root, "plug_id"),
			getRequiredInt(root, "household_id"),
			getRequiredInt(root, "house_id")
		);
	}

	private int getRequiredInt(JsonNode root, String fieldName) {
		JsonNode node = root.get(fieldName);

		if (node == null) {
			throw new IllegalArgumentException("Missing field: " + fieldName);
		}

		try {
			return Integer.parseInt(node.asText());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid integer field: " + fieldName, e);
		}
	}

	private long getRequiredLong(JsonNode root, String fieldName) {
		JsonNode node = root.get(fieldName);
		if (node == null || !node.canConvertToLong()) {
			throw new IllegalArgumentException("Missing or invalid field: " + fieldName);
		}
		return node.asLong();
	}

	private double getRequiredDouble(JsonNode root, String fieldName) {
		JsonNode node = root.get(fieldName);
		if (node == null) {
        	throw new IllegalArgumentException("Missing field: " + fieldName);
		}

		try {
			return Double.parseDouble(node.asText());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid double field: " + fieldName, e);
		}
	}

	/**
	 * Parsed MQTT event that is ready to be emitted into Storm.
	 */
	private static final class LoadEvent {

		private final long id;
		private final long timestamp;
		private final double value;
		private final int plugId;
		private final int householdId;
		private final int houseId;

		private LoadEvent(long id, long timestamp, double value, int plugId, int householdId, int houseId) {
			this.id = id;
			this.timestamp = timestamp;
			this.value = value;
			this.plugId = plugId;
			this.householdId = householdId;
			this.houseId = houseId;
		}
	}
}
