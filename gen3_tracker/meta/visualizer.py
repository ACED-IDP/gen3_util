from gen3_tracker.meta import aggregate
from pyvis.network import Network


def _container():
    """Create a pyvis container."""
    return Network(notebook=True, cdn_resources='in_line')  # filter_menu=True, select_menu=True


def _load(net: Network, aggregation: dict) -> Network:
    """Load the aggregation into the visualization network."""
    # add vertices
    for resource_type, _ in aggregation.items():
        assert 'count' in _, _
        net.add_node(resource_type, label=f"{resource_type}/{_['count']}")
    # add edges
    for resource_type, _ in aggregation.items():
        for ref in _.get('references', {}):
            count = _['references'][ref]['count']
            net.add_edge(resource_type, ref, title=count, value=count)
    return net


def create_network_graph(directory_path: str, output_path: str):
    """Render metadata as a network graph into output_path.

    \b
    directory_path: The directory path to the metadata.
    output_path: The path to save the network graph.
    """
    aggregation = aggregate(directory_path)
    # Load it into a pyvis
    net = _load(_container(), aggregation)
    net.save_graph(str(output_path))
    net.show_buttons(filter_=['physics'])
