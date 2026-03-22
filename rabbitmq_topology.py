"""Build an in-memory RabbitMQ topology graph from definitions exports."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional


def _normalize_vhost(vhost: Any) -> str:
    if isinstance(vhost, str) and vhost:
        return vhost
    return "/"


def _safe_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _as_items(value: Any) -> Iterable[Mapping[str, Any]]:
    if not isinstance(value, list):
        return
    for item in value:
        if isinstance(item, Mapping):
            yield item


def _node_id(kind: str, name: str, vhost: str) -> str:
    return f"{kind}:{vhost}:{name}"


@dataclass
class TopologyNode:
    # The type of node, e.g. "exchange" or "queue".
    kind_: str

    name_: str
    vhost_: str = "/"
    durable_: Optional[bool] = None
    auto_delete_: Optional[bool] = None
    internal_: Optional[bool] = None
    exchange_type_: Optional[str] = None
    arguments_: Dict[str, Any] = field(default_factory=dict)

    # The raw definition from which this node was created, if available.
    raw_: Dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return _node_id(self.kind_, self.name_, self.vhost_)


@dataclass
class BindingEdge:
    source_id_: str
    destination_id_: str
    source_name_: str
    destination_name_: str
    destination_type_: str
    vhost_: str = "/"
    routing_key_: str = ""
    arguments_: Dict[str, Any] = field(default_factory=dict)
    raw_: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TopologyGraph:
    # Indexed by node ID (kind:vhost:name)
    nodes_: Dict[str, TopologyNode] = field(default_factory=dict)

    edges_: List[BindingEdge] = field(default_factory=list)

    outgoing_: Dict[str, List[int]] = field(default_factory=dict)
    incoming_: Dict[str, List[int]] = field(default_factory=dict)

    @classmethod
    def from_file(cls, definition_file: str | Path) -> "TopologyGraph":
        path = Path(definition_file)
        with path.open("r", encoding="utf-8") as handle:
            definitions = json.load(handle)
        return cls.from_definitions(definitions)

    @classmethod
    def from_definitions(cls, definitions: Mapping[str, Any]) -> "TopologyGraph":
        graph = cls()

        for exchange in _as_items(definitions.get("exchanges")):
            graph.add_exchange(exchange)

        for queue in _as_items(definitions.get("queues")):
            graph.add_queue(queue)

        for binding in _as_items(definitions.get("bindings")):
            graph.add_binding(binding)

        return graph

    def add_exchange(self, exchange: Mapping[str, Any]) -> TopologyNode:
        name = str(exchange.get("name", ""))
        vhost = _normalize_vhost(exchange.get("vhost"))
        return self._upsert_node(kind="exchange", name=name, vhost=vhost, raw=exchange)

    def add_queue(self, queue: Mapping[str, Any]) -> TopologyNode:
        name = str(queue.get("name", ""))
        vhost = _normalize_vhost(queue.get("vhost"))
        return self._upsert_node(kind="queue", name=name, vhost=vhost, raw=queue)

    def add_binding(self, binding: Mapping[str, Any]) -> Optional[BindingEdge]:
        source_name = str(binding.get("source", ""))
        destination_name = str(binding.get("destination", ""))
        destination_type = str(binding.get("destination_type", ""))
        vhost = _normalize_vhost(binding.get("vhost"))

        if destination_type not in {"exchange", "queue"}:
            return None

        source_node = self._upsert_node(
            kind="exchange",
            name=source_name,
            vhost=vhost,
        )
        destination_node = self._upsert_node(
            kind=destination_type,
            name=destination_name,
            vhost=vhost,
        )

        edge = BindingEdge(
            source_id_=source_node.id,
            destination_id_=destination_node.id,
            source_name_=source_name,
            destination_name_=destination_name,
            destination_type_=destination_type,
            vhost_=vhost,
            routing_key_=str(binding.get("routing_key", "")),
            arguments_=_safe_dict(binding.get("arguments")),
            raw_=dict(binding),
        )

        edge_index = len(self.edges_)
        self.edges_.append(edge)
        self.outgoing_.setdefault(source_node.id, []).append(edge_index)
        self.incoming_.setdefault(destination_node.id, []).append(edge_index)
        return edge

    def get_node(self, kind: str, name: str, vhost: str = "/") -> Optional[TopologyNode]:
        return self.nodes_.get(_node_id(kind, name, _normalize_vhost(vhost)))

    def outbound_edges(
        self, kind: str, name: str, vhost: str = "/"
    ) -> List[BindingEdge]:
        node_id = _node_id(kind, name, _normalize_vhost(vhost))
        return [self.edges_[index] for index in self.outgoing_.get(node_id, [])]

    def inbound_edges(self, kind: str, name: str, vhost: str = "/") -> List[BindingEdge]:
        node_id = _node_id(kind, name, _normalize_vhost(vhost))
        return [self.edges_[index] for index in self.incoming_.get(node_id, [])]

    def adjacency(self) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for source_id, edge_indexes in self.outgoing_.items():
            result[source_id] = [self.edges_[index].destination_id_ for index in edge_indexes]
        return result

    def summary(self) -> Dict[str, int]:
        exchange_count = sum(1 for node in self.nodes_.values() if node.kind_ == "exchange")
        queue_count = sum(1 for node in self.nodes_.values() if node.kind_ == "queue")
        return {
            "total_nodes": len(self.nodes_),
            "exchange_nodes": exchange_count,
            "queue_nodes": queue_count,
            "binding_edges": len(self.edges_),
        }

    def _upsert_node(
        self,
        *,
        kind: str,
        name: str,
        vhost: str,
        raw: Optional[Mapping[str, Any]] = None,
    ) -> TopologyNode:
        node_id = _node_id(kind, name, vhost)
        existing = self.nodes_.get(node_id)

        if existing is None:
            raw_dict = _safe_dict(raw)
            node = TopologyNode(
                kind_=kind,
                name_=name,
                vhost_=vhost,
                durable_=raw_dict.get("durable"),
                auto_delete_=raw_dict.get("auto_delete"),
                internal_=raw_dict.get("internal"),
                exchange_type_=raw_dict.get("type"),
                arguments_=_safe_dict(raw_dict.get("arguments")),
                raw_=raw_dict,
            )
            self.nodes_[node_id] = node
            return node

        if raw:
            raw_dict = _safe_dict(raw)
            existing.durable_ = raw_dict.get("durable", existing.durable_)
            existing.auto_delete_ = raw_dict.get("auto_delete", existing.auto_delete_)
            existing.internal_ = raw_dict.get("internal", existing.internal_)
            existing.exchange_type_ = raw_dict.get("type", existing.exchange_type_)
            if raw_dict.get("arguments"):
                existing.arguments_ = _safe_dict(raw_dict.get("arguments"))
            if raw_dict:
                existing.raw_ = raw_dict

        return existing


def build_topology_graph(definitions: Mapping[str, Any]) -> TopologyGraph:
    return TopologyGraph.from_definitions(definitions)


def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Build an in-memory topology graph from a RabbitMQ definitions file.",
    )
    parser.add_argument("definitions_file", type=Path, help="Path to definitions JSON file")
    args = parser.parse_args()



if __name__ == "__main__":
    _main()