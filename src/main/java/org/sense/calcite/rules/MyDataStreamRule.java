package org.sense.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel;

public class MyDataStreamRule extends RelOptRule {
	// @formatter:off
	// public static final MyDataStreamRule INSTANCE = new MyDataStreamRule(RelFactories.LOGICAL_BUILDER);
	public static final MyDataStreamRule INSTANCE = new MyDataStreamRule(operand(DataStreamRel.class, none()), "MyDataStreamRule");

	public MyDataStreamRule(RelOptRuleOperand operand, String description) {
		super(operand, "MyDataStreamRule:" + description);
	}

	public MyDataStreamRule(RelBuilderFactory relBuilderFactory) {
		super(operand(DataStreamRel.class, any()), relBuilderFactory, null);
	}

	public void onMatch(RelOptRuleCall call) {
		DataStreamRel dataStreamRel = (DataStreamRel) call.rel(0);
		System.out.println("======================= MyDataStreamRule.onMatch ====================");
		/*
		final Filter topFilter = call.rel(0);
		final Filter bottomFilter = call.rel(1);

		// use RexPrograms to merge the two FilterRels into a single program
		// so we can convert the two LogicalFilter conditions to directly
		// reference the bottom LogicalFilter's child
		RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
		RexProgram bottomProgram = createProgram(bottomFilter);
		RexProgram topProgram = createProgram(topFilter);

		RexProgram mergedProgram = RexProgramBuilder.mergePrograms(topProgram, bottomProgram, rexBuilder);

		RexNode newCondition = mergedProgram.expandLocalRef(mergedProgram.getCondition());

		final RelBuilder relBuilder = call.builder();
		relBuilder.push(bottomFilter.getInput()).filter(newCondition);

		call.transformTo(relBuilder.build());
		*/
		// final RelBuilder relBuilder = call.builder();
		// call.transformTo(relBuilder.build());
	}
	// @formatter:on
}
