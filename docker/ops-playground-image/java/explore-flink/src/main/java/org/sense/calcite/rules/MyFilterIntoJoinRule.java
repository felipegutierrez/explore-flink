package org.sense.calcite.rules;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;
// import org.apache.flink.table.plan.util.FlinkRelOptUtil;

public class MyFilterIntoJoinRule extends RelOptRule {
	public static final MyFilterIntoJoinRule INSTANCE = new MyFilterIntoJoinRule(Filter.class, Join.class,
			RelFactories.LOGICAL_BUILDER);

	/**
	 * Predicate that always returns true. With this predicate, every filter will be
	 * pushed into the ON clause.
	 */
	public static final Predicate TRUE_PREDICATE = (join, joinType, exp) -> true;

	/**
	 * Predicate that returns whether a filter is valid in the ON clause of a join
	 * for this particular kind of join. If not, Calcite will push it back to above
	 * the join.
	 */
	private final Predicate predicate;

	public MyFilterIntoJoinRule(Class<Filter> filterClazz, Class<Join> joinClazz, RelBuilderFactory relBuilderFactory) {
		this(filterClazz, joinClazz, relBuilderFactory, TRUE_PREDICATE);
	}

	public MyFilterIntoJoinRule(Class<Filter> filterClazz, Class<Join> joinClazz, RelBuilderFactory relBuilderFactory,
			Predicate predicate) {
		super(RelOptRule.operand(filterClazz, RelOptRule.operand(joinClazz, RelOptRule.any())), relBuilderFactory,
				null);
		this.predicate = Objects.requireNonNull(predicate);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		System.out.println("======================= MyFilterIntoJoinRule.onMatch ====================");
		Filter filter = call.rel(0);
		Join join = call.rel(1);
		perform(call, filter, join);
	}

	protected void perform(RelOptRuleCall call, Filter filter, Join join) {
		final List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
		// final List<RexNode> origJoinFilters =
		// com.google.common.collect.ImmutableList.copyOf(joinFilters);
		final List<RexNode> origJoinFilters = ImmutableList.copyOf(joinFilters);

		// If there is only the joinRel,
		// make sure it does not match a cartesian product joinRel
		// (with "true" condition), otherwise this rule will be applied
		// again on the new cartesian product joinRel.
		if (filter == null && joinFilters.isEmpty()) {
			return;
		}

		final List<RexNode> aboveFilters = filter != null ? conjunctions(filter.getCondition()) : new ArrayList<>();
		// final com.google.common.collect.ImmutableList<RexNode> origAboveFilters =
		// com.google.common.collect.ImmutableList.copyOf(aboveFilters);
		final ImmutableList<RexNode> origAboveFilters = ImmutableList.copyOf(aboveFilters);

		// Simplify Outer Joins
		JoinRelType joinType = join.getJoinType();
		if (!origAboveFilters.isEmpty() && join.getJoinType() != JoinRelType.INNER) {
			// joinType = FlinkRelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
		}

		final List<RexNode> leftFilters = new ArrayList<>();
		final List<RexNode> rightFilters = new ArrayList<>();

		// TODO - add logic to derive additional filters. E.g., from
		// (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
		// derive table filters:
		// (t1.a = 1 OR t1.b = 3)
		// (t2.a = 2 OR t2.b = 4)

		// Try to push down above filters. These are typically where clause
		// filters. They can be pushed down if they are not on the NULL
		// generating side.
		boolean filterPushed = false;
		// if (FlinkRelOptUtil.classifyFilters(join, aboveFilters, joinType, !(join
		// instanceof EquiJoin),
		// !joinType.generatesNullsOnLeft(), !joinType.generatesNullsOnRight(),
		// joinFilters, leftFilters,
		// rightFilters)) {
		filterPushed = true;
		// }

		// Move join filters up if needed
		validateJoinFilters(aboveFilters, joinFilters, join, joinType);

		// If no filter got pushed after validate, reset filterPushed flag
		if (leftFilters.isEmpty() && rightFilters.isEmpty() && joinFilters.size() == origJoinFilters.size()) {
			// if
			// (com.google.common.collect.Sets.newHashSet(joinFilters).equals(com.google.common.collect.Sets.newHashSet(origJoinFilters)))
			// {
			if (Sets.newHashSet(joinFilters).equals(Sets.newHashSet(origJoinFilters))) {
				filterPushed = false;
			}
		}

		// boolean isAntiJoin = joinType == JoinRelType.ANTI;

		// Try to push down filters in ON clause. A ON clause filter can only be
		// pushed down if it does not affect the non-matching set, i.e. it is
		// not on the side which is preserved.
		// A ON clause filter of anti-join can not be pushed down.
		// if (!isAntiJoin && FlinkRelOptUtil.classifyFilters(join, joinFilters,
		// joinType, false,
		// !joinType.generatesNullsOnRight(), !joinType.generatesNullsOnLeft(),
		// joinFilters, leftFilters,
		// rightFilters)) {
		filterPushed = true;
		// }

		// if nothing actually got pushed and there is nothing leftover,
		// then this rule is a no-op
		if ((!filterPushed && joinType == join.getJoinType())
				|| (joinFilters.isEmpty() && leftFilters.isEmpty() && rightFilters.isEmpty())) {
			return;
		}

		// create Filters on top of the children if any filters were
		// pushed to them
		final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
		final RelBuilder relBuilder = call.builder();
		final RelNode leftRel = relBuilder.push(join.getLeft()).filter(leftFilters).build();
		final RelNode rightRel = relBuilder.push(join.getRight()).filter(rightFilters).build();

		// create the new join node referencing the new children and
		// containing its new join filters (if there are any)
		// final com.google.common.collect.ImmutableList<RelDataType> fieldTypes =
		// com.google.common.collect.ImmutableList
		final ImmutableList<RelDataType> fieldTypes = ImmutableList.<RelDataType>builder()
				.addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
				.addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build();
		final RexNode joinFilter = RexUtil.composeConjunction(rexBuilder,
				RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes), true);

		// If nothing actually got pushed and there is nothing leftover,
		// then this rule is a no-op
		if (joinFilter.isAlwaysTrue() && leftFilters.isEmpty() && rightFilters.isEmpty()
				&& joinType == join.getJoinType()) {
			return;
		}

		RelNode newJoinRel = join.copy(join.getTraitSet(), joinFilter, leftRel, rightRel, joinType,
				join.isSemiJoinDone());
		call.getPlanner().onCopy(join, newJoinRel);
		if (!leftFilters.isEmpty()) {
			call.getPlanner().onCopy(filter, leftRel);
		}
		if (!rightFilters.isEmpty()) {
			call.getPlanner().onCopy(filter, rightRel);
		}

		relBuilder.push(newJoinRel);

		// Create a project on top of the join if some of the columns have become
		// NOT NULL due to the join-type getting stricter.
		relBuilder.convert(join.getRowType(), false);

		// create a FilterRel on top of the join if needed
		relBuilder.filter(
				RexUtil.fixUp(rexBuilder, aboveFilters, RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));

		call.transformTo(relBuilder.build());
	}

	/**
	 * Validates that target execution framework can satisfy join filters.
	 *
	 * <p>
	 * If the join filter cannot be satisfied (for example, if it is
	 * {@code l.c1 > r.c2} and the join only supports equi-join), removes the filter
	 * from {@code joinFilters} and adds it to {@code aboveFilters}.
	 *
	 * <p>
	 * The default implementation does nothing; i.e. the join can handle all
	 * conditions.
	 *
	 * @param aboveFilters Filter above Join
	 * @param joinFilters  Filters in join condition
	 * @param join         Join
	 * @param joinType     JoinRelType could be different from type in Join due to
	 *                     outer join simplification.
	 */
	protected void validateJoinFilters(List<RexNode> aboveFilters, List<RexNode> joinFilters, Join join,
			JoinRelType joinType) {
		final Iterator<RexNode> filterIter = joinFilters.iterator();
		while (filterIter.hasNext()) {
			RexNode exp = filterIter.next();
			if (!predicate.apply(join, joinType, exp)) {
				aboveFilters.add(exp);
				filterIter.remove();
			}
		}
	}

	/**
	 * Predicate that returns whether a filter is valid in the ON clause of a join
	 * for this particular kind of join. If not, Calcite will push it back to above
	 * the join.
	 */
	public interface Predicate {
		boolean apply(Join join, JoinRelType joinType, RexNode exp);
	}
}
