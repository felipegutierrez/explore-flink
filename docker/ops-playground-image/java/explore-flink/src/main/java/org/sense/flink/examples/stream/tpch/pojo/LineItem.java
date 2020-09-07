package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;

public class LineItem extends io.airlift.tpch.LineItem implements Serializable {
    private static final long serialVersionUID = 1L;

    public LineItem(long rowNumber, long orderKey, long partKey, long supplierKey, int lineNumber, long quantity,
                    long extendedPrice, long discount, long tax, String returnFlag, String status, int shipDate,
                    int commitDate, int receiptDate, String shipInstructions, String shipMode, String comment) {
        super(rowNumber, orderKey, partKey, supplierKey, lineNumber, quantity, extendedPrice, discount, tax, returnFlag,
                status, shipDate, commitDate, receiptDate, shipInstructions, shipMode, comment);
    }

    @Override
    public String toString() {
        return "LineItem [RowNumber=" + getRowNumber() + ", OrderKey=" + getOrderKey() + ", PartKey="
                + getPartKey() + ", SupplierKey=" + getSupplierKey() + ", LineNumber=" + getLineNumber()
                + ", Quantity=" + getQuantity() + ", ExtendedPrice=" + getExtendedPrice()
                + ", ExtendedPriceInCents=" + getExtendedPriceInCents() + ", Discount=" + getDiscount()
                + ", DiscountPercent=" + getDiscountPercent() + ", Tax=" + getTax() + ", TaxPercent="
                + getTaxPercent() + ", ReturnFlag=" + getReturnFlag() + ", Status=" + getStatus()
                + ", ShipDate=" + getShipDate() + ", CommitDate=" + getCommitDate() + ", ReceiptDate="
                + getReceiptDate() + ", ShipInstructions=" + getShipInstructions() + ", ShipMode="
                + getShipMode() + ", Comment=" + getComment() + "]";
    }
}
