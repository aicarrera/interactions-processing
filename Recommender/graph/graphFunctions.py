import logging

import networkx as nx
from pprint import pprint
import matplotlib.pyplot as plt


def add_nodes_to_graph_ngrams(seqs, N):
    model = {}

    for s in seqs:
        s = tuple(s)
        for Nth in range(1, N + 1):
            for i in range(len(s) - Nth):
                ngram = tuple(s[i:i + Nth])  # get a slice of length N from current position
                next_item = tuple([s[i + Nth]])
                dic_values = model.setdefault(ngram, {})
                dic_values.setdefault(next_item, 0)
                dic_values[next_item] += 1

    for k, d in model.items():
        tot = sum(list(d.values()))
        for k, v in d.items():
            d[k] = round((v / tot), 4)
    print("Model:", model)
    e= open("model.csv","w", encoding="utf8")
    for k1, v1 in model.items():
        for k2,v2 in v1.items():
            e.write("{},{},{}\n".format(k1,k2,v2))
    e.close()
    # create graph object
    G = nx.DiGraph()

    # nodes correspond to states
    G.add_nodes_from(list(model.keys()))
    logging.debug(f'Nodes:\n{G.nodes()}\n')

    # edges represent transition probabilities
    for k, v in model.items():
        for kval, vval in v.items():
            G.add_edge(k, kval, probability=vval, label=vval)

    return (model, G)


def show_graph(G):
    print(G.edges(data=True))

    pos = nx.spring_layout(G, seed=15)  # positions for all nodes - seed for reproducibility
    colors = range(len(G.nodes))
    # nodes
    nx.draw_networkx_nodes(G, pos, node_size=1000, node_color=colors, cmap=plt.cm.tab20c)

    # node labels
    nx.draw_networkx_labels(G, pos, font_size=6, font_family="sans-serif")
    # edge weight labels
    nx.draw_networkx_edges(G, pos, width=0.25, edge_color="tab:blue", alpha=0.5, style="dashed", arrows=True,
                           arrowstyle="-", )
    labels = {(n1, n2): d['probability'] for n1, n2, d in G.edges(data=True)}

    edge_labels = nx.draw_networkx_edge_labels(G, pos=nx.spring_layout(G, seed=15), edge_labels=labels, font_size=5)

    ax = plt.gca()
    ax.margins(0.02)

    plt.tight_layout()
    plt.show()
