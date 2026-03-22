"""Visualization helpers for RabbitMQ topology graphs."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional

from rabbitmq_topology import TopologyGraph


def _dot_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _is_topology_graph(graph: Any) -> bool:
    return hasattr(graph, "nodes_") and hasattr(graph, "edges_")


def _is_rabbit_topology(graph: Any) -> bool:
    return hasattr(graph, "graph") and hasattr(graph, "exchanges") and hasattr(graph, "queues")


def topology_graph_to_dot(
    graph: Any,
    *,
    vhost: Optional[str] = None,
    include_vhost_in_label: bool = False,
    include_routing_keys: bool = True,
) -> str:
    """Convert a TopologyGraph or RabbitTopology into Graphviz DOT text."""
    lines = [
        "digraph RabbitMQTopology {",
        "  rankdir=LR;",
        '  graph [fontname="Helvetica"];',
        '  node [fontname="Helvetica", style="filled", fillcolor="white"];',
        '  edge [fontname="Helvetica", color="#666666"];',
    ]

    if _is_rabbit_topology(graph):
        return _rabbit_topology_to_dot(
            graph,
            vhost=vhost,
            include_vhost_in_label=include_vhost_in_label,
            include_routing_keys=include_routing_keys,
        )

    if not _is_topology_graph(graph):
        raise TypeError("graph must be a TopologyGraph or RabbitTopology instance")

    visible_nodes = set()
    for node_id in sorted(graph.nodes_):
        node = graph.nodes_[node_id]
        if vhost is not None and node.vhost_ != vhost:
            continue

        visible_nodes.add(node_id)
        shape = "ellipse" if node.kind_ == "exchange" else "box"
        color = "#CFE8FF" if node.kind_ == "exchange" else "#D7F7D0"

        label = node.name_
        if include_vhost_in_label:
            label = f"{label}\\n({node.vhost_})"

        lines.append(
            f'  "{_dot_escape(node_id)}" [label="{_dot_escape(label)}", '
            f'shape={shape}, fillcolor="{color}"];'
        )

    for edge in graph.edges_:
        if vhost is not None and edge.vhost_ != vhost:
            continue
        if edge.source_id_ not in visible_nodes or edge.destination_id_ not in visible_nodes:
            continue

        attrs = []
        if include_routing_keys and edge.routing_key_:
            attrs.append(f'label="{_dot_escape(edge.routing_key_)}"')

        attrs_text = f" [{', '.join(attrs)}]" if attrs else ""
        lines.append(
            f'  "{_dot_escape(edge.source_id_)}" -> '
            f'"{_dot_escape(edge.destination_id_)}"{attrs_text};'
        )

    lines.append("}")
    return "\n".join(lines)


def _rabbit_topology_to_dot(
    graph: Any,
    *,
    vhost: Optional[str] = None,
    include_vhost_in_label: bool = False,
    include_routing_keys: bool = True,
) -> str:
    lines = [
        "digraph RabbitMQTopology {",
        "  rankdir=LR;",
        '  graph [fontname="Helvetica"];',
        '  node [fontname="Helvetica", style="filled", fillcolor="white"];',
        '  edge [fontname="Helvetica", color="#666666"];',
    ]

    for name in sorted(graph.graph.nodes):
        attrs = graph.graph.nodes[name]
        node_type = attrs.get("node_type", "queue")
        shape = "ellipse" if node_type == "exchange" else "box"
        color = "#CFE8FF" if node_type == "exchange" else "#D7F7D0"

        label = name
        if include_vhost_in_label:
            label = f"{label}\\n(/)"

        lines.append(
            f'  "{_dot_escape(str(name))}" [label="{_dot_escape(str(label))}", '
            f'shape={shape}, fillcolor="{color}"];'
        )

    for source, destination, attrs in graph.graph.edges(data=True):
        if vhost is not None:
            # RabbitTopology does not track per-node vhost, so only root vhost can be filtered.
            if vhost != "/":
                continue

        edge_attrs = []
        routing_key = attrs.get("routing_key", "")
        if include_routing_keys and routing_key:
            edge_attrs.append(f'label="{_dot_escape(str(routing_key))}"')

        attrs_text = f" [{', '.join(edge_attrs)}]" if edge_attrs else ""
        lines.append(
            f'  "{_dot_escape(str(source))}" -> '
            f'"{_dot_escape(str(destination))}"{attrs_text};'
        )

    lines.append("}")
    return "\n".join(lines)


def visualize_topology_graph(
    graph: Any,
    output_file: str | Path,
    *,
    vhost: Optional[str] = None,
    include_vhost_in_label: bool = False,
    include_routing_keys: bool = True,
    graphviz_command: str = "dot",
) -> Path:
    """Write a visual representation of a topology graph to disk.

    If output_file ends with .dot, the DOT source is written directly.
    For other extensions (.png, .svg, .pdf, ...), this function invokes Graphviz.
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dot_text = topology_graph_to_dot(
        graph,
        vhost=vhost,
        include_vhost_in_label=include_vhost_in_label,
        include_routing_keys=include_routing_keys,
    )

    if output_path.suffix.lower() == ".dot":
        output_path.write_text(dot_text, encoding="utf-8")
        return output_path

    if not output_path.suffix:
        raise ValueError("output_file must include a file extension (for example: .dot, .svg, .png)")

    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".dot", delete=False, encoding="utf-8") as tmp:
            tmp.write(dot_text)
            tmp_path = Path(tmp.name)

        output_format = output_path.suffix.lstrip(".")
        subprocess.run(
            [graphviz_command, f"-T{output_format}", str(tmp_path), "-o", str(output_path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Graphviz command '{graphviz_command}' was not found. "
            "Install Graphviz or write to a .dot file instead."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(f"Graphviz rendering failed: {stderr}") from exc
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    return output_path
