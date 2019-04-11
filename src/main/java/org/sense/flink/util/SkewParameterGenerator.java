package org.sense.flink.util;

import java.io.Serializable;

/**
 * <pre>
 * 1 - Create a list of numbers.
 * 2 - Iterate the list sequentially and infinitely.
 * 3 - Restart the iteration once offset arrive on the last position.
 * </pre>
 * 
 * @author Felipe Oliveira Gutierrez
 */
public class SkewParameterGenerator implements Serializable {

	private static final long serialVersionUID = 110669606780724943L;
	private Integer[] list;
	private int currentPosition;

	public SkewParameterGenerator(int size) {
		createList(size);
	}

	private void createList(int size) {
		list = new Integer[size];
		for (int i = 0; i < list.length; i++) {
			list[i] = new Integer(i + 1);
		}
		currentPosition = 0;
	}

	public Integer getNextItem() {
		Integer nextItem = list[currentPosition];
		if (currentPosition < list.length - 1) {
			currentPosition++;
		} else {
			currentPosition = 0;
		}
		return nextItem;
	}
}
