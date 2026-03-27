"""Visualization helpers for RabbitMQ topology graphs."""

from __future__ import annotations

import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from xml.etree import ElementTree as ET

def _dot_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _is_topology_graph(graph: Any) -> bool:
    return hasattr(graph, "nodes_") and hasattr(graph, "edges_")


def _is_rabbit_topology(graph: Any) -> bool:
    return hasattr(graph, "graph_") and hasattr(graph, "exchanges_") and hasattr(graph, "queues_")


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

    for name in sorted(graph.graph_.nodes):
        attrs = graph.graph_.nodes[name]
        node_type = attrs.get("node_type", "queue")
        if node_type == "exchange":
            shape = "ellipse"
            color = "#CFE8FF"
        elif node_type == "shovel":
            shape = "diamond"
            color = "#FFE8CF"
        else:
            shape = "box"
            color = "#D7F7D0"

        label = name
        if include_vhost_in_label:
            label = f"{label}\\n(/)"

        lines.append(
            f'  "{_dot_escape(str(name))}" [label="{_dot_escape(str(label))}", '
            f'shape={shape}, fillcolor="{color}"];'
        )

    for source, destination, attrs in graph.graph_.edges(data=True):
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


def _drawio_style_for_node_type(node_type: str) -> str:
    if node_type == "exchange":
        return "rounded=1;whiteSpace=wrap;html=1;fillColor=#CFE8FF;strokeColor=#5B8DB8;"
    if node_type == "shovel":
        return "rhombus;whiteSpace=wrap;html=1;fillColor=#FFE8CF;strokeColor=#B88646;"
    return "rounded=0;whiteSpace=wrap;html=1;fillColor=#D7F7D0;strokeColor=#5E9C55;"


def topology_graph_to_drawio(
    graph: Any,
    *,
    include_vhost_in_label: bool = False,
    include_routing_keys: bool = True,
    diagram_name: str = "RabbitMQ Topology",
) -> str:
    """Convert a TopologyGraph or RabbitTopology into draw.io XML text."""
    node_rows: list[tuple[str, str, str]] = []
    edge_rows: list[tuple[str, str, str]] = []

    if _is_rabbit_topology(graph):
        for name in sorted(graph.graph_.nodes):
            attrs = graph.graph_.nodes[name]
            node_type = attrs.get("node_type", "queue")
            label = str(name)
            if include_vhost_in_label:
                label = f"{label}\\n(/)"
            node_rows.append((str(name), str(node_type), label))

        for source, destination, attrs in graph.graph_.edges(data=True):
            routing_key = str(attrs.get("routing_key", ""))
            edge_rows.append((str(source), str(destination), routing_key))
    elif _is_topology_graph(graph):
        for node_id in sorted(graph.nodes_):
            node = graph.nodes_[node_id]
            label = node.name_
            if include_vhost_in_label:
                label = f"{label}\\n({node.vhost_})"
            node_rows.append((node_id, node.kind_, label))

        for edge in graph.edges_:
            edge_rows.append((edge.source_id_, edge.destination_id_, edge.routing_key_))
    else:
        raise TypeError("graph must be a TopologyGraph or RabbitTopology instance")

    mxfile = ET.Element(
        "mxfile",
        {
            "host": "app.diagrams.net",
            "modified": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "agent": "rabbitchk",
            "version": "24.7.17",
        },
    )
    diagram = ET.SubElement(mxfile, "diagram", {"id": "rabbit-topology", "name": diagram_name})
    model = ET.SubElement(
        diagram,
        "mxGraphModel",
        {
            "dx": "1200",
            "dy": "900",
            "grid": "1",
            "gridSize": "10",
            "guides": "1",
            "tooltips": "1",
            "connect": "1",
            "arrows": "1",
            "fold": "1",
            "page": "1",
            "pageScale": "1",
            "pageWidth": "1700",
            "pageHeight": "1100",
            "math": "0",
            "shadow": "0",
        },
    )
    root = ET.SubElement(model, "root")
    ET.SubElement(root, "mxCell", {"id": "0"})
    ET.SubElement(root, "mxCell", {"id": "1", "parent": "0"})

    x_by_type = {
        "exchange": 40,
        "queue": 360,
        "shovel": 680,
        "other": 1000,
    }
    y_index_by_type: dict[str, int] = {"exchange": 0, "queue": 0, "shovel": 0, "other": 0}
    node_id_map: dict[str, str] = {}

    next_id = 2
    for node_name, node_type, label in node_rows:
        drawio_node_type = node_type if node_type in x_by_type else "other"
        x = x_by_type[drawio_node_type]
        y = 40 + y_index_by_type[drawio_node_type] * 90
        y_index_by_type[drawio_node_type] += 1

        cell_id = str(next_id)
        next_id += 1
        node_id_map[node_name] = cell_id

        cell = ET.SubElement(
            root,
            "mxCell",
            {
                "id": cell_id,
                "value": label,
                "style": _drawio_style_for_node_type(drawio_node_type),
                "vertex": "1",
                "parent": "1",
            },
        )
        ET.SubElement(
            cell,
            "mxGeometry",
            {
                "x": str(x),
                "y": str(y),
                "width": "220",
                "height": "60",
                "as": "geometry",
            },
        )

    for source, destination, routing_key in edge_rows:
        source_id = node_id_map.get(source)
        destination_id = node_id_map.get(destination)
        if not source_id or not destination_id:
            continue

        label = routing_key if include_routing_keys else ""
        if include_routing_keys and not routing_key:
            label = ""

        edge_cell = ET.SubElement(
            root,
            "mxCell",
            {
                "id": str(next_id),
                "value": label,
                "style": "edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;endArrow=block;endFill=1;",
                "edge": "1",
                "parent": "1",
                "source": source_id,
                "target": destination_id,
            },
        )
        next_id += 1
        ET.SubElement(edge_cell, "mxGeometry", {"relative": "1", "as": "geometry"})

    xml_text = ET.tostring(mxfile, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_text}'


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

    if output_path.suffix.lower() == ".drawio":
        drawio_text = topology_graph_to_drawio(
            graph,
            include_vhost_in_label=include_vhost_in_label,
            include_routing_keys=include_routing_keys,
        )
        output_path.write_text(drawio_text, encoding="utf-8")
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
