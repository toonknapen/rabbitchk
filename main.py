
import argparse
import rabbit_topology
import topology_visualization

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load and print a RabbitMQ topology from a JSON file.")
    parser.add_argument("--definitions-file", type=str, help="Path to the JSON file containing the topology data.")
    parser.add_argument("--output-file", type=str, help="Path a graphic file to write the topology visualization to (e.g. .png, .pdf).")
    args = parser.parse_args()

    topology = rabbit_topology.RabbitTopology()
    topology.load_from_json_file(args.definitions_file)
    print(topology.summary())

    if args.output_file:
        topology_visualization.visualize_topology_graph(topology, args.output_file)
        print("Topology visualization written to:", args.output_file)