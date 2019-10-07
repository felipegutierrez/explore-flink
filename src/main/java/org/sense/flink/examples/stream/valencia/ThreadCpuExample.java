package org.sense.flink.examples.stream.valencia;

import static org.junit.Assert.assertEquals;

import java.util.BitSet;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class ThreadCpuExample {
	public static void main(String[] args) {
		int nbits = Runtime.getRuntime().availableProcessors();
		BitSet affinity0 = LinuxJNAAffinity.INSTANCE.getAffinity();
		System.out.println(affinity0);

		BitSet affinity = new BitSet(nbits);

		affinity.set(3);
		LinuxJNAAffinity.INSTANCE.setAffinity(affinity);
		BitSet affinity2 = LinuxJNAAffinity.INSTANCE.getAffinity();
		System.out.println(affinity2);
		System.out.println("getCpu: " + LinuxJNAAffinity.INSTANCE.getCpu());
		assertEquals(3, LinuxJNAAffinity.INSTANCE.getCpu());
		assertEquals(affinity, affinity2);

		affinity.set(0, nbits);
		LinuxJNAAffinity.INSTANCE.setAffinity(affinity);
	}
}
