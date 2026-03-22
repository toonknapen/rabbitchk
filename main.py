
import argparse
import rabbit_topology
import topology_visualization

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load and print a RabbitMQ topology from a JSON file.")
    parser.add_argument("--definitions-file", type=str, help="Path to the JSON file containing the topology data.")
    parser.add_argument("--substract", type=str, help="Path to the JSON file containing the topology data to compare for subgraph.")
    parser.add_argument("--output-file", type=str, help="Path a graphic file to write the topology visualization to (e.g. .png, .pdf).")
    args = parser.parse_args()

    topology = rabbit_topology.RabbitTopology()
    topology.load_from_json_file(args.definitions_file)
    print("Topology loaded from:", args.definitions_file)
    print(topology.summary())

    # test if --substract is provided, and if so, load the other topology and compare them
    if args.substract: 
        other_topology = rabbit_topology.RabbitTopology()
        other_topology.load_from_json_file(args.substract)
        print("Other topology loaded from:", args.substract)
        print(other_topology.summary())

        diff = topology.substract(other_topology)
        if diff.is_empty():
            print("The first topology is a subgraph of the second topology.")
        else:
            print("The first topology is NOT a subgraph of the second topology.")
            print("Differences:")
            print(diff.summary()) 

    if args.output_file:
        topology_visualization.visualize_topology_graph(topology, args.output_file)
        print("Topology visualization written to:", args.output_file)