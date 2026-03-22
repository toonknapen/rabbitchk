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
    
    Nodes are exchanges, queues, or shovels.
    Edges represent bindings between exchanges and queues, or shovels connecting queues.
    """
    
    def __init__(self):
        """Initialize an empty topology graph."""
        self.graph_: nx.DiGraph = nx.DiGraph()
        self.exchanges_: Dict[str, Dict[str, Any]] = {}  # original exchange definitions
        self.queues_: Dict[str, Dict[str, Any]] = {}     # original queue definitions
        self.bindings_: Dict[Tuple[str, str], Dict[str, Any]] = {}  # keyed by (source, destination)
        self.shovels_: Dict[str, Dict[str, Any]] = {}    # original shovel definitions
    
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
            data: Dictionary containing 'exchanges', 'queues', 'bindings', and optionally 'parameters'
        """
        # Clear existing data
        self.graph_.clear()
        self.exchanges_.clear()
        self.queues_.clear()
        self.bindings_.clear()
        self.shovels_.clear()
        
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
        
        # Add shovels (from parameters with component type 'shovel')
        if 'parameters' in data:
            for param in data['parameters']:
                if param.get('component') == 'shovel':
                    self._add_shovel(param)
    
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
        
        self.bindings_[(source, destination)] = dict(binding)
        
        # Create edge from exchange to queue
        self.graph_.add_edge(
            source,
            destination,
            routing_key=routing_key,
            binding_type=binding.get('destination_type', 'queue')
        )
    
    def _add_shovel(self, shovel: Dict[str, Any]) -> None:
        """Add a shovel node and edges to the graph."""
        name = shovel['name']
        self.shovels_[name] = shovel
        
        # Add shovel as a node
        self.graph_.add_node(
            name,
            node_type='shovel'
        )
        
        # Extract source and destination info from shovel value
        value = shovel.get('value', {})
        src_queue = value.get('src-queue')
        dest_address = value.get('dest-address')
        
        # Add edge from source queue to shovel
        if src_queue:
            self.graph_.add_edge(
                src_queue,
                name,
                binding_type='shovel'
            )
        
        # Add edge from shovel to destination address
        if dest_address:
            self.graph_.add_edge(
                name,
                dest_address,
                binding_type='shovel'
            )
    def is_empty(self) -> bool:
        """Check if the topology graph is empty."""
        return self.graph_.number_of_nodes() == 0 and self.graph_.number_of_edges() == 0
    
    def has_exchange(self, exchange: Dict[str,Any]) -> bool: 
        name = exchange['name']
        return exchange == self.exchanges_.get(name)
    
    def has_queue(self, queue: Dict[str,Any]) -> bool:
        name = queue['name']
        return queue == self.queues_.get(name)
    
    def has_binding(self, binding: Dict[str, Any]) -> bool:
        """Check if a specific binding exists."""
        key = (binding['source'], binding['destination'])
        return binding == self.bindings_.get(key)
    
    def has_shovel(self, shovel: Dict[str, Any]) -> bool:
        """Check if a specific shovel exists."""
        name = shovel['name']
        return shovel == self.shovels_.get(name)

    def substract(self, other: 'RabbitTopology') -> RabbitTopology:
        """Check if this topology is a subgraph of another topology."""
        diff = RabbitTopology()

        # Check if all exchanges, queues, bindings, and shovels in this graph are present in the other graph
        for exchange in self.exchanges_.values():
            if not other.has_exchange(exchange):
                diff._add_exchange(exchange)
        
        for queue in self.queues_.values():
            if not other.has_queue(queue):
                diff._add_queue(queue)
        
        for binding in self.bindings_.values():
            if not other.has_binding(binding):
                diff._add_binding(binding)
        
        for shovel in self.shovels_.values():
            if not other.has_shovel(shovel):
                diff._add_shovel(shovel)
        
        return diff
    
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
        lines.append(f"Shovels: {len(self.shovels_)}")
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
            for binding in self.bindings_.values():
                source = binding['source']
                dest = binding['destination']
                routing_key = binding.get('routing_key', '')
                key_str = f" (key: '{routing_key}')" if routing_key else ""
                lines.append(f"  - {source} -> {dest}{key_str}")
        
        if self.shovels_:
            lines.append("")
            lines.append("Shovels:")
            for name, shovel in self.shovels_.items():
                value = shovel.get('value', {})
                src_queue = value.get('src-queue', 'unknown')
                dest_address = value.get('dest-address', 'unknown')
                lines.append(f"  - {name}: {src_queue} => {dest_address}")
        
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
