package org.sense.flink.util;

import org.apache.flink.util.FlinkException;

/**
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class CustomFlinkException extends FlinkException {

	private static final long serialVersionUID = -6789189728061631683L;

	public CustomFlinkException() {
		super("This is my Custom Exception for Flink");
	}

	public CustomFlinkException(String message) {
		super(message);
	}

	public CustomFlinkException(String message, Throwable cause) {
		super(message, cause);
	}

	public CustomFlinkException(Throwable cause) {
		super(cause);
	}
}
