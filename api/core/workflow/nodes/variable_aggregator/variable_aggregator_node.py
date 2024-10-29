from collections.abc import Mapping, Sequence
from typing import Any

from core.workflow.entities.node_entities import NodeRunResult
from core.workflow.nodes.base import BaseNode
from core.workflow.nodes.enums import NodeType
from core.workflow.nodes.variable_aggregator.entities import VariableAssignerNodeData
from models.workflow import WorkflowNodeExecutionStatus


class VariableAggregatorNode(BaseNode[VariableAssignerNodeData]):
    _node_data_cls = VariableAssignerNodeData
    _node_type = NodeType.VARIABLE_AGGREGATOR

    def _run(self) -> NodeRunResult:
        # Get variables
        outputs = {}
        inputs = {}

        if not self.node_data.advanced_settings or not self.node_data.advanced_settings.group_enabled:
            outputs = {"output": []}
            for selector in self.node_data.variables:
                variable = self.graph_runtime_state.variable_pool.get(selector)
                if variable is not None:
                    item = variable.to_object()
                    if isinstance(item, list):
                        outputs["output"].extend(item)
                    else:
                        outputs["output"].append(item)
                    inputs[".".join(selector[1:])] = variable.to_object()

        else:
            for group in self.node_data.advanced_settings.groups:
                outputs[group.group_name] = {"output": []}
                for selector in group.variables:
                    variable = self.graph_runtime_state.variable_pool.get(selector)
                    if variable is not None:
                        item = variable.to_object()
                        if isinstance(item, list):
                            outputs[group.group_name]["output"].extend(item)
                        else:
                            outputs[group.group_name]["output"].append(item)
                        inputs[".".join(selector[1:])] = variable.to_object()

        return NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED, outputs=outputs, inputs=inputs)

    @classmethod
    def _extract_variable_selector_to_variable_mapping(
        cls, *, graph_config: Mapping[str, Any], node_id: str, node_data: VariableAssignerNodeData
    ) -> Mapping[str, Sequence[str]]:
        """
        Extract variable selector to variable mapping
        :param graph_config: graph config
        :param node_id: node id
        :param node_data: node data
        :return:
        """
        return {}
