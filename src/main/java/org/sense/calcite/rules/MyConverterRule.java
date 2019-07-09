package org.sense.calcite.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * based on class the ConvertToNotInOrInRule
 * @author Felipe Oliveira Gutierrez
 */
public class MyConverterRule extends RelOptRule {
	public static final MyConverterRule INSTANCE = new MyConverterRule(Filter.class, RelFactories.LOGICAL_BUILDER);

	private SqlBinaryOperator toOperator;
	private SqlBinaryOperator fromOperator;
	private SqlBinaryOperator connectOperator;
	private SqlBinaryOperator composedOperator;

	private MyConverterRule(Class<? extends Filter> filterClazz, RelBuilderFactory relBuilderFactory) {
		super(RelOptRule.operand(filterClazz, RelOptRule.any()), relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		System.out.println("======================= MyConverterRule.onMatch ====================");
		final Filter filter = (Filter) call.rel(0);
		RexNode condition = filter.getCondition();

		RexNode rexNode = convertToNotInOrIn(call.builder(), condition, SqlStdOperatorTable.IN);
	}

	private RexNode convertToNotInOrIn(RelBuilder builder, RexNode rexCondition, SqlBinaryOperator toOperator) {
		if (toOperator.equals(SqlStdOperatorTable.IN)) {
			fromOperator = SqlStdOperatorTable.EQUALS;
			connectOperator = SqlStdOperatorTable.OR;
			composedOperator = SqlStdOperatorTable.AND;
		} else if (toOperator.equals(SqlStdOperatorTable.NOT_IN)) {
			fromOperator = SqlStdOperatorTable.NOT_EQUALS;
			connectOperator = SqlStdOperatorTable.AND;
			composedOperator = SqlStdOperatorTable.OR;
		} else {
			System.out.println("");
		}
		List<RexNode> decomposed = decomposedBy(rexCondition, connectOperator);
		Map<String, List<RexCall>> combineMap = new HashMap<String, List<RexCall>>();
		List<RexNode> rexBuffer = new ArrayList<RexNode>();

		for (RexNode rexNode : decomposed) {
			if (rexNode instanceof RexCall) {
				RexCall rexCall = (RexCall) rexNode;
				SqlOperator sql = rexCall.getOperator();
				if (sql == SqlStdOperatorTable.EQUALS || sql == SqlStdOperatorTable.NOT_EQUALS) {
					List<RexNode> operands = rexCall.getOperands();
					if (operands.get(1) instanceof RexLiteral) {
						String key = operands.get(0).toString();
						if (combineMap.containsKey(key)) {
							return operands.get(0);
						} else {
							combineMap.put(key, new ArrayList<RexCall>(Arrays.asList(rexCall)));
						}
					} else if (operands.get(0) instanceof RexLiteral) {
						String key = operands.get(1).toString();
						if (combineMap.containsKey(key)) {
							return operands.get(1);
						} else {
							// rexCall.clone(type, operands)
							combineMap.put(key, new ArrayList<RexCall>(Arrays.asList(rexCall)));
						}
					} else {
						rexBuffer.add(rexCall);
					}
				} else if (sql == SqlStdOperatorTable.AND || sql == SqlStdOperatorTable.OR) {
					List<RexNode> newRex = decomposedBy(rexCall, composedOperator);
				} else {
					rexBuffer.add(rexCall);
				}
			} else {
				rexBuffer.add(rexNode);
			}
		}
		return null;
	}

	private List<RexNode> decomposedBy(RexNode rexPredicate, SqlBinaryOperator operator) {
		if (operator.equals(SqlStdOperatorTable.AND)) {
			return RelOptUtil.conjunctions(rexPredicate);
		} else if (operator.equals(SqlStdOperatorTable.OR)) {
			return RelOptUtil.disjunctions(rexPredicate);
		}
		return null;
	}
}
