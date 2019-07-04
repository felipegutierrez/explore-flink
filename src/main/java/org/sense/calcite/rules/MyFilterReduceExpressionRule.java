package org.sense.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.tools.RelBuilderFactory;

public class MyFilterReduceExpressionRule extends RelOptRule {

	public static final MyFilterReduceExpressionRule FILTER_INSTANCE = new MyFilterReduceExpressionRule(
			operand(LogicalFilter.class, none()), "MyFilterReduceExpressionRule");

	public MyFilterReduceExpressionRule(RelOptRuleOperand operand, String description) {
		super(operand, "MyFilterReduceExpressionRule:" + description);
	}

	public MyFilterReduceExpressionRule(RelBuilderFactory relBuilderFactory) {
		super(operand(LogicalFilter.class, any()), relBuilderFactory, null);
	}

	public MyFilterReduceExpressionRule(RelOptRuleOperand operand) {
		super(operand);
	}

	@Override
	public void onMatch(RelOptRuleCall arg0) {
		System.out.println("======================= MyFilterReduceExpressionRule.onMatch ====================");
	}
}
