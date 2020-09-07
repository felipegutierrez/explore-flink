package org.sense.calcite.rules

import org.apache.calcite.plan.RelOptRule.{ operand, _ }
import org.apache.calcite.plan.{ RelOptRule, RelOptRuleCall }
import org.apache.flink.table.plan.nodes.datastream._

import scala.collection.JavaConverters._

object MyDataStreamRetractionRulesScala {

  /**
   * Rule instance that assigns default retraction to [[DataStreamRel]] nodes.
   */
  val DEFAULT_RETRACTION_INSTANCE = new MyAssignDefaultRetractionRuleScala()

  class MyAssignDefaultRetractionRuleScala extends RelOptRule(operand(
    classOf[DataStreamRel], none()),
    "MyAssignDefaultRetractionRuleScala") {
    override def onMatch(call: RelOptRuleCall): Unit = {
      val rel = call.rel(0).asInstanceOf[DataStreamRel]
      val traits = rel.getTraitSet

      val traitsWithUpdateAsRetraction =
        if (null == traits.getTrait(UpdateAsRetractionTraitDef.INSTANCE)) {
          traits.plus(UpdateAsRetractionTrait.DEFAULT)
        } else {
          traits
        }
      val traitsWithAccMode =
        if (null == traitsWithUpdateAsRetraction.getTrait(AccModeTraitDef.INSTANCE)) {
          traitsWithUpdateAsRetraction.plus(AccModeTrait.DEFAULT)
        } else {
          traitsWithUpdateAsRetraction
        }

      if (traits != traitsWithAccMode) {
        call.transformTo(rel.copy(traitsWithAccMode, rel.getInputs))
      }
    }
  }
}
