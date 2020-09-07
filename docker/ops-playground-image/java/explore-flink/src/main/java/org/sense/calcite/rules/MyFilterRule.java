package org.sense.calcite.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class MyFilterRule extends RelOptRule {
	public static final MyFilterRule INSTANCE = new MyFilterRule(Filter.class, RelFactories.LOGICAL_BUILDER);

	private MyFilterRule(Class<? extends Filter> filterClazz, RelBuilderFactory relBuilderFactory) {
		super(RelOptRule.operand(filterClazz, RelOptRule.any()), relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		System.out.println("======================= MyFilterRule.onMatch ====================");
		final Filter filter = (Filter) call.rel(0);

		final RelNode inputFilter = filter.getInput();
		final MyFilter myFilter = new MyFilter(inputFilter.getCluster(), inputFilter.getTraitSet(), inputFilter,
				filter.getCondition());

		RexNode condition = filter.getCondition();

		System.out.println();
		call.transformTo(myFilter);
	}

	private static class MyFilter extends Filter {

		MyFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
			super(cluster, traitSet, child, condition);
		}

		public MyFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
			return new MyFilter(getCluster(), traitSet, input, condition);
		}
	}
}
