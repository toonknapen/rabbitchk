
import argparse
import json
import rabbitmq_topology
import topology_visualization

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load and print a RabbitMQ topology from a JSON file.")
    parser.add_argument("--definitions-file", type=str, help="Path to the JSON file containing the topology data.")
    parser.add_argument("--output-file", type=str, help="Path a graphic file to write the topology visualization to (e.g. .png, .pdf).")
    args = parser.parse_args()

    graph = rabbitmq_topology.TopologyGraph.from_file(args.definitions_file)
    print(json.dumps(graph.summary(), indent=2, sort_keys=True))

    if args.output_file:
        topology_visualization.visualize_topology_graph(graph, args.output_file)
        print("Topology visualization written to:", args.output_file)