package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;
import java.util.Date;

public class LineItem extends io.airlift.tpch.LineItem implements Serializable {
	private static final long serialVersionUID = 1L;
	private long timestamp;

	public LineItem(long rowNumber, long orderKey, long partKey, long supplierKey, int lineNumber, long quantity,
			long extendedPrice, long discount, long tax, String returnFlag, String status, int shipDate, int commitDate,
			int receiptDate, String shipInstructions, String shipMode, String comment) {
		super(rowNumber, orderKey, partKey, supplierKey, lineNumber, quantity, extendedPrice, discount, tax, returnFlag,
				status, shipDate, commitDate, receiptDate, shipInstructions, shipMode, comment);
		this.timestamp = new Date().getTime();
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "LineItem [getTimestamp()=" + getTimestamp() + ", getRowNumber()=" + getRowNumber() + ", getOrderKey()="
				+ getOrderKey() + ", getPartKey()=" + getPartKey() + ", getSupplierKey()=" + getSupplierKey()
				+ ", getLineNumber()=" + getLineNumber() + ", getQuantity()=" + getQuantity() + ", getExtendedPrice()="
				+ getExtendedPrice() + ", getExtendedPriceInCents()=" + getExtendedPriceInCents() + ", getDiscount()="
				+ getDiscount() + ", getDiscountPercent()=" + getDiscountPercent() + ", getTax()=" + getTax()
				+ ", getTaxPercent()=" + getTaxPercent() + ", getReturnFlag()=" + getReturnFlag() + ", getStatus()="
				+ getStatus() + ", getShipDate()=" + getShipDate() + ", getCommitDate()=" + getCommitDate()
				+ ", getReceiptDate()=" + getReceiptDate() + ", getShipInstructions()=" + getShipInstructions()
				+ ", getShipMode()=" + getShipMode() + ", getComment()=" + getComment() + "]";
	}

}
