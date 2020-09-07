package org.sense.flink.examples.stream.udf;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

/**
 * Base interface for clean up state, both for {@link ProcessFunction} and {@link CoProcessFunction}.
 */
public interface CleanupState {

	default void registerProcessingCleanupTimer(
			ValueState<Long> cleanupTimeState,
			long currentTime,
			long minRetentionTime,
			long maxRetentionTime,
			TimerService timerService) throws Exception {

		// last registered timer
		Long curCleanupTime = cleanupTimeState.value();

		// check if a cleanup timer is registered and
		// that the current cleanup timer won't delete state we need to keep
		if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
			// we need to register a new (later) timer
			long cleanupTime = currentTime + maxRetentionTime;
			// register timer and remember clean-up time
			timerService.registerProcessingTimeTimer(cleanupTime);
			// delete expired timer
			if (curCleanupTime != null) {
				timerService.deleteProcessingTimeTimer(curCleanupTime);
			}
			cleanupTimeState.update(cleanupTime);
		}
	}
}
