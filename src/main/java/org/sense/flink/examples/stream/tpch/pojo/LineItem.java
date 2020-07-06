package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;

public class LineItem extends io.airlift.tpch.LineItem implements Serializable {
	private static final long serialVersionUID = 1L;

	public LineItem(long rowNumber, long orderKey, long partKey, long supplierKey, int lineNumber, long quantity,
			long extendedPrice, long discount, long tax, String returnFlag, String status, int shipDate, int commitDate,
			int receiptDate, String shipInstructions, String shipMode, String comment) {
		super(rowNumber, orderKey, partKey, supplierKey, lineNumber, quantity, extendedPrice, discount, tax, returnFlag,
				status, shipDate, commitDate, receiptDate, shipInstructions, shipMode, comment);
	}

	@Override
	public String toString() {
		return "LineItem [getRowNumber()=" + getRowNumber() + ", getOrderKey()=" + getOrderKey() + ", getPartKey()="
				+ getPartKey() + ", getSupplierKey()=" + getSupplierKey() + ", getLineNumber()=" + getLineNumber()
				+ ", getQuantity()=" + getQuantity() + ", getExtendedPrice()=" + getExtendedPrice()
				+ ", getExtendedPriceInCents()=" + getExtendedPriceInCents() + ", getDiscount()=" + getDiscount()
				+ ", getDiscountPercent()=" + getDiscountPercent() + ", getTax()=" + getTax() + ", getTaxPercent()="
				+ getTaxPercent() + ", getReturnFlag()=" + getReturnFlag() + ", getStatus()=" + getStatus()
				+ ", getShipDate()=" + getShipDate() + ", getCommitDate()=" + getCommitDate() + ", getReceiptDate()="
				+ getReceiptDate() + ", getShipInstructions()=" + getShipInstructions() + ", getShipMode()="
				+ getShipMode() + ", getComment()=" + getComment() + "]";
	}
}
