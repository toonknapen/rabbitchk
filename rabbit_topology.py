"""
RabbitMQ Topology Graph Module

This module provides functionality to load RabbitMQ definition files and build
an in-memory graph representation of exchanges and queues with their bindings.
"""

import json
from typing import Dict, List, Any, Optional, Set, Tuple
import networkx as nx


class RabbitTopology:
    """
    Represents a RabbitMQ topology as a directed graph.
    
    Nodes are either exchanges or queues.
    Edges represent bindings between exchanges and queues.
    """
    
    def __init__(self):
        """Initialize an empty topology graph."""
        self.graph_: nx.DiGraph = nx.DiGraph()
        self.exchanges_: Dict[str, Dict[str, Any]] = {}
        self.queues_: Dict[str, Dict[str, Any]] = {}
        self.bindings_: List[Dict[str, Any]] = []
    
    def load_from_json_file(self, filepath: str) -> None:
        """
        Load RabbitMQ topology from a JSON definition file.
        
        Args:
            filepath: Path to the RabbitMQ definition JSON file
        """
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        self.load_from_dict(data)
    
    def load_from_dict(self, data: Dict[str, Any]) -> None:
        """
        Load RabbitMQ topology from a dictionary.
        
        Args:
            data: Dictionary containing 'exchanges', 'queues', and 'bindings'
        """
        # Clear existing data
        self.graph_.clear()
        self.exchanges_.clear()
        self.queues_.clear()
        self.bindings_.clear()
        
        # Add exchanges
        if 'exchanges' in data:
            for exchange in data['exchanges']:
                self._add_exchange(exchange)
        
        # Add queues
        if 'queues' in data:
            for queue in data['queues']:
                self._add_queue(queue)
        
        # Add bindings
        if 'bindings' in data:
            for binding in data['bindings']:
                self._add_binding(binding)
    
    def _add_exchange(self, exchange: Dict[str, Any]) -> None:
        """Add an exchange node to the graph."""
        name = exchange['name']
        self.exchanges_[name] = exchange
        self.graph_.add_node(
            name,
            node_type='exchange',
            exchange_type=exchange.get('type', 'unknown'),
            durable=exchange.get('durable', False),
            auto_delete=exchange.get('auto_delete', False),
            internal=exchange.get('internal', False)
        )
    
    def _add_queue(self, queue: Dict[str, Any]) -> None:
        """Add a queue node to the graph."""
        name = queue['name']
        self.queues_[name] = queue
        self.graph_.add_node(
            name,
            node_type='queue',
            durable=queue.get('durable', False),
            auto_delete=queue.get('auto_delete', False)
        )
    
    def _add_binding(self, binding: Dict[str, Any]) -> None:
        """Add a binding edge to the graph."""
        source = binding['source']
        destination = binding['destination']
        routing_key = binding.get('routing_key', '')
        
        self.bindings_.append(binding)
        
        # Create edge from exchange to queue
        self.graph_.add_edge(
            source,
            destination,
            routing_key=routing_key,
            binding_type=binding.get('destination_type', 'queue')
        )
    
    def get_exchanges(self) -> Dict[str, Dict[str, Any]]:
        """Get all exchanges."""
        return self.exchanges_.copy()
    
    def get_queues(self) -> Dict[str, Dict[str, Any]]:
        """Get all queues."""
        return self.queues_.copy()
    
    def get_bindings(self) -> List[Dict[str, Any]]:
        """Get all bindings."""
        return self.bindings_.copy()
    
    def get_graph(self) -> nx.DiGraph:
        """Get the underlying NetworkX directed graph."""
        return self.graph_
    
    def get_exchange_bindings(self, exchange_name: str) -> List[str]:
        """
        Get all queues bound to a specific exchange.
        
        Args:
            exchange_name: Name of the exchange
            
        Returns:
            List of queue names bound to the exchange
        """
        if exchange_name not in self.graph_:
            return []
        
        return list(self.graph_.successors(exchange_name))
    
    def get_queue_sources(self, queue_name: str) -> List[str]:
        """
        Get all exchanges that bind to a specific queue.
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            List of exchange names that bind to the queue
        """
        if queue_name not in self.graph_:
            return []
        
        return list(self.graph_.predecessors(queue_name))
    
    def get_routing_key(self, exchange_name: str, queue_name: str) -> Optional[str]:
        """
        Get the routing key for a specific exchange-queue binding.
        
        Args:
            exchange_name: Name of the exchange
            queue_name: Name of the queue
            
        Returns:
            Routing key if binding exists, None otherwise
        """
        if self.graph_.has_edge(exchange_name, queue_name):
            return self.graph_[exchange_name][queue_name].get('routing_key')
        return None
    
    def get_connected_components(self) -> List[Set[str]]:
        """
        Get all connected components in the topology.
        
        Returns:
            List of sets, each containing nodes in a connected component
        """
        undirected = self.graph_.to_undirected()
        components = list(nx.connected_components(undirected))
        return components
    
    def summary(self) -> str:
        """
        Get a text summary of the topology.
        
        Returns:
            Multi-line string describing the topology
        """
        lines = []
        lines.append(f"RabbitMQ Topology Summary")
        lines.append(f"{'=' * 50}")
        lines.append(f"Exchanges: {len(self.exchanges_)}")
        lines.append(f"Queues: {len(self.queues_)}")
        lines.append(f"Bindings: {len(self.bindings_)}")
        lines.append(f"")
        
        if self.exchanges_:
            lines.append("Exchanges:")
            for name, props in self.exchanges_.items():
                lines.append(f"  - {name} ({props.get('type', 'unknown')})")
        
        if self.queues_:
            lines.append("")
            lines.append("Queues:")
            for name in sorted(self.queues_.keys()):
                lines.append(f"  - {name}")
        
        if self.bindings_:
            lines.append("")
            lines.append("Bindings:")
            for binding in self.bindings_:
                source = binding['source']
                dest = binding['destination']
                routing_key = binding.get('routing_key', '')
                key_str = f" (key: '{routing_key}')" if routing_key else ""
                lines.append(f"  - {source} -> {dest}{key_str}")
        
        return "\n".join(lines)


def main():
    """Example usage of the RabbitMQTopologyGraph."""
    # Example: Load from a JSON file
    topology = RabbitTopology()
    topology.load_from_json_file('data/mine.json')
    
    print(topology.summary())
    print("\nGraph Stats:")
    print(f"Nodes: {topology.graph_.number_of_nodes()}")
    print(f"Edges: {topology.graph_.number_of_edges()}")


if __name__ == "__main__":
    main()
