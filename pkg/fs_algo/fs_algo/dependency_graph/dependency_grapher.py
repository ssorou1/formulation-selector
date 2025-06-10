import ast
import argparse
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
from pyvis.network import Network

class FunctionCallVisitor(ast.NodeVisitor):
    def __init__(self):
        self.calls = {}  # {caller_function: [callee1, callee2, ...]}
        self.current_function = None

    def visit_FunctionDef(self, node):
        self.current_function = node.name
        self.calls.setdefault(node.name, [])
        self.generic_visit(node)
        self.current_function = None

    def visit_Call(self, node):
        if self.current_function:
            if isinstance(node.func, ast.Name):
                called_func = node.func.id
                self.calls[self.current_function].append(called_func)
            elif isinstance(node.func, ast.Attribute):
                # Handles object.method() cases if you want to include these
                called_func = node.func.attr
                self.calls[self.current_function].append(called_func)
        self.generic_visit(node)

def build_dependency_graph(file_path):
    source = Path(file_path).read_text()
    tree = ast.parse(source)

    visitor = FunctionCallVisitor()
    visitor.visit(tree)

    # Filter out external calls (only include those defined in file)
    defined_functions = set(visitor.calls.keys())
    edges = [(caller, callee) for caller, callees in visitor.calls.items()
             for callee in callees if callee in defined_functions]

    # Build graph
    G = nx.DiGraph()
    G.add_edges_from(edges)
    return G

def draw_graph(G, title="Function Dependency Graph", html_output="function_graph.html"):
    html_path = Path(html_output)
    png_output = html_path.with_suffix('.png')
    
    plt.figure(figsize=(16, 12))

    try:
        from networkx.drawing.nx_agraph import graphviz_layout
        pos = graphviz_layout(G, prog='dot')
    except ImportError:
        print("pygraphviz not installed, using spring layout.")
        pos = nx.spring_layout(G, k=1.5, iterations=100)

    nx.draw(G, pos, with_labels=True, node_color="skyblue",
            node_size=2500, font_size=10, font_weight='bold', edge_color="gray")

    plt.title(title)
    plt.tight_layout()
    plt.savefig(png_output)  # Optional static image
    plt.show()

    # Draw interactive graph using pyvis
    net = Network(height="750px", width="100%", directed=True, notebook=False)
    net.barnes_hut()

    for node in G.nodes():
        net.add_node(node, label=node)

    for source, target in G.edges():
        net.add_edge(source, target)

    net.set_options("""
    var options = {
      "edges": {
        "arrows": {
          "to": {
            "enabled": true
          }
        },
        "smooth": false
      },
      "layout": {
        "hierarchical": {
          "enabled": true,
          "direction": "UD",
          "sortMethod": "hubsize"
        }
      },
      "physics": {
        "hierarchicalRepulsion": {
          "nodeDistance": 120
        },
        "minVelocity": 0.75
      }
    }
    """)
    
    net.show(html_output, notebook=False)
    print(f"Interactive graph saved as: {html_output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate function dependency graph for a Python file.")
    parser.add_argument("file_path", type=str, help="Path to the Python file to analyze")
    parser.add_argument("--html_output", type=str, help="Filename for the interactive HTML output")

    args = parser.parse_args()
    # args.file_path = f'~/git/formulation-selector/pkg/fs_algo/fs_algo/fs_algo_train_eval.py'    

    file_path = Path(args.file_path)
    if args.html_output:
        html_output = args.html_output
    else:
        html_output = f"{file_path.stem}_dependency_graph.html"

    G = build_dependency_graph(file_path)
    draw_graph(G, title="Function Dependency Graph", html_output=html_output)
