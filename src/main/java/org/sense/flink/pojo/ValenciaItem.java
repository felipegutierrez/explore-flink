package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public abstract class ValenciaItem implements Serializable {
	private static final long serialVersionUID = -107439991877659718L;
	protected Integer id;
	protected Date update;
	protected List<Point> coordinates;

}
