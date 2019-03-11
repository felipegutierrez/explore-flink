package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;

public class MultiSensorMultiStationsJoinMqtt {

	public MultiSensorMultiStationsJoinMqtt() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		DataStream<MqttSensor> streamStation01People = env.addSource(new MqttSensorConsumer("topic-station-01-people"));
		DataStream<MqttSensor> streamStation01Trains = env.addSource(new MqttSensorConsumer("topic-station-01-trains"));
		DataStream<MqttSensor> streamStation01Tickets = env.addSource(new MqttSensorConsumer("topic-station-01-tickets"));
		DataStream<MqttSensor> streamStation02People = env.addSource(new MqttSensorConsumer("topic-station-02-people"));
		DataStream<MqttSensor> streamStation02Trains = env.addSource(new MqttSensorConsumer("topic-station-02-trains"));
		DataStream<MqttSensor> streamStation02Tickets = env.addSource(new MqttSensorConsumer("topic-station-02-tickets"));

		// Join sensor counter from PEOPLE and TICKETS from the same PLATFORM
		streamStation01People.join(streamStation01Tickets)
				.where(new PlatformTypeKeySelector())
				.equalTo(new PlatformTypeKeySelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.apply(new PeopleAndTicketOnSamePlatformJoinFunction())
				.print();

		// Join sensor counter from TICKETS and TRAINs from the same PLATFORM in order to discover if we need a bigger train
		streamStation01Tickets.join(streamStation01Trains)
				.where(new PlatformTypeKeySelector())
				.equalTo(new PlatformTypeKeySelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.apply(new TicketAndTrainOnSamePlatformJoinFunction())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("MultiSensorMultiStationsReadingMqtt2");
	}

	/**
	 * Key Selector by platform ID
	 */
	public static class PlatformTypeKeySelector implements KeySelector<MqttSensor, Integer> {

		private static final long serialVersionUID = 3696259925817763986L;

		@Override
		public Integer getKey(MqttSensor value) throws Exception {
			return value.getKey().f2;
		}
	}

	/**
	 * Join function to People and Ticket sensor readings
	 */
	public static class PeopleAndTicketOnSamePlatformJoinFunction
			implements JoinFunction<MqttSensor, MqttSensor, String> {

		private static final long serialVersionUID = -4817380281365483097L;

		@Override
		public String join(MqttSensor people, MqttSensor tickets) throws Exception {

			String ids = people.getKey().f0 + "," + tickets.getKey().f0;
			String sensorType = people.getKey().f1 + ", " + tickets.getKey().f1;
			String platformId = people.getKey().f2 + ", " + tickets.getKey().f2;
			String platformType = people.getKey().f3 + ", " + tickets.getKey().f3;
			String stations = people.getKey().f4 + ", " + tickets.getKey().f4;

			String result = "ID[" + ids + "] sensorTypes[" + sensorType + "] platformId[" + platformId
					+ "] platformType[" + platformType + "] stations[" + stations + "] people[" + people.getValue()
					+ "] tickets[" + tickets.getValue() + "]";
			if (people.getValue() > tickets.getValue()) {
				// people are not paying tickets
				return "People are not paying for tickets. " + result;
			} else if (people.getValue() < tickets.getValue()) {
				// there are more tickets than people
				return "There are more tickets than people. " + result;
			} else {
				// everything is fine
				return "Nice. Nobody is frauding. " + result;
			}
		}
	}

	/**
	 * Join TICKETS and TRAINS from the same platform in order to discover if it is
	 * necessary to send a bigger train. Let's suppose that each train has the
	 * capacity of 200 people.
	 */
	public static class TicketAndTrainOnSamePlatformJoinFunction
			implements JoinFunction<MqttSensor, MqttSensor, String> {

		private static final long serialVersionUID = -857500653881800605L;
		private final Integer trainCapacity = 200;

		@Override
		public String join(MqttSensor tickets, MqttSensor trains) throws Exception {

			String ids = tickets.getKey().f0 + "," + trains.getKey().f0;
			String sensorType = tickets.getKey().f1 + ", " + trains.getKey().f1;
			String platformId = tickets.getKey().f2 + ", " + trains.getKey().f2;
			String platformType = tickets.getKey().f3 + ", " + trains.getKey().f3;
			String stations = tickets.getKey().f4 + ", " + trains.getKey().f4;

			String result = "ID[" + ids + "] sensorTypes[" + sensorType + "] tickets[" + tickets.getValue()
					+ "] trains[" + trains.getValue() + "] platformId[" + platformId + "] platformType[" + platformType
					+ "] stations[" + stations + "]";

			Double totalCapacity = trains.getValue() * trainCapacity;
			Double almostOverflowing = totalCapacity - ((totalCapacity / 100) * 10);
			Double almostEmpty = totalCapacity - ((totalCapacity / 100) * 90);

			if (tickets.getValue() > almostOverflowing && tickets.getValue() <= totalCapacity) {
				return "The trains are almost overflowing of people (between 90% and 100%). " + result;
			}
			if (tickets.getValue() > totalCapacity) {
				return "The trains are overflowing of people (>100%). " + result;
			}
			if (tickets.getValue() <= almostEmpty) {
				return "The trains are almost empty (<10%). " + result;
			}
			return "The trains are running on a good capacity (betwen 10% and 90%). " + result;
		}
	}

}
