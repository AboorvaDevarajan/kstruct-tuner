import pandas as pd
import argparse
import json
import numpy as np
from typing import Dict, List, Tuple, Any


def hierarchical_clustering(headers: List[str], matrix) -> List[str]:
    n = len(headers)
    clusters = {i: [headers[i]] for i in range(n)}
    edges = [(matrix[i, j], i, j) for i in range(n) for j in range(i + 1, n) if matrix[i, j] > 0]
    edges.sort(reverse=True, key=lambda x: x[0])

    for weight, i, j in edges:
        cluster_i = cluster_j = None
        for cluster_id, members in clusters.items():
            if headers[i] in members:
                cluster_i = cluster_id
            if headers[j] in members:
                cluster_j = cluster_id
            if cluster_i is not None and cluster_j is not None:
                break

        if cluster_i != cluster_j:
            clusters[cluster_i].extend(clusters[cluster_j])
            del clusters[cluster_j]
    
    reordered_indices = [idx for cluster in clusters.values() for idx in range(n) if headers[idx] in cluster]
    return [headers[i] for i in reordered_indices]


def compute_top_pairs(df: pd.DataFrame, top_n: int) -> List[Tuple[str, str, float]]:
    pairs = []
    cols = df.columns.tolist()
    values = df.to_numpy()
    n = len(cols)
    for i in range(n):
        for j in range(i + 1, n):
            w = float(values[i, j])
            if w > 0:
                pairs.append((cols[i], cols[j], w))
    pairs.sort(key=lambda x: x[2], reverse=True)
    return pairs[:top_n]


def compute_field_strengths(df: pd.DataFrame) -> List[Tuple[str, float]]:
    strengths = (df.sum(axis=1) + df.sum(axis=0)) / 2.0
    return sorted([(field, float(strengths[field])) for field in df.columns], key=lambda x: x[1], reverse=True)


def compute_intercluster_links(df: pd.DataFrame, clusters: Dict[int, List[str]], top_n: int) -> List[Tuple[str, str, float]]:
    results: List[Tuple[str, str, float]] = []
    for cid_a, members_a in clusters.items():
        for cid_b, members_b in clusters.items():
            if cid_a >= cid_b:
                continue
            sub = df.loc[members_a, members_b]
            max_val = float(sub.to_numpy().max()) if not sub.empty else 0.0
            results.append((f"Cluster {cid_a}", f"Cluster {cid_b}", max_val))
    results.sort(key=lambda x: x[2], reverse=True)
    return results[:top_n]


def main():
    parser = argparse.ArgumentParser(
        description="Perform hierarchical clustering on an adjacency matrix CSV and write insights to a file."
    )
    parser.add_argument("input_csv", help="Path to input adjacency matrix CSV (rows/cols are field names).")
    parser.add_argument(
        "--output_file",
        "-o",
        default="reordering_insights.txt",
        help="Path to write insights (reordered fields and clusters).",
    )
    parser.add_argument(
        "--top_n",
        type=int,
        default=20,
        help="How many top pairs and inter-cluster links to include.",
    )
    parser.add_argument(
        "--json_out",
        type=str,
        default=None,
        help="Optional path to also write insights as JSON.",
    )
    parser.add_argument(
        "--per_field_top_k",
        type=int,
        default=3,
        help="Top-k partners to report per field in JSON/text.",
    )

    args = parser.parse_args()

    df = pd.read_csv(args.input_csv, index_col=0)
    headers = df.columns.tolist()
    matrix = df.to_numpy()

    reordered_fields = hierarchical_clustering(headers, matrix)

    with open(args.output_file, "w") as f:
        f.write("Reordered Fields (suggested layout)\n")
        f.write(", ".join(reordered_fields) + "\n\n")

        f.write("Clusters (order reflects merge sequence; for reference)\n")
  
        # Rerun minimal clustering to capture the final cluster grouping
        n = len(headers)
        clusters = {i: [headers[i]] for i in range(n)}
        edges = [(matrix[i, j], i, j) for i in range(n) for j in range(i + 1, n) if matrix[i, j] > 0]
        edges.sort(reverse=True, key=lambda x: x[0])
        for _, i, j in edges:
            ci = cj = None
            for cid, members in clusters.items():
                if headers[i] in members:
                    ci = cid
                if headers[j] in members:
                    cj = cid
                if ci is not None and cj is not None:
                    break
            if ci != cj:
                clusters[ci].extend(clusters[cj])
                del clusters[cj]

        for cid, members in clusters.items():
            f.write(f"Cluster {cid}: " + ", ".join(members) + "\n")

        # Additional insights
        f.write("\nTop Field Pairs (by co-access weight)\n")
        top_pairs = compute_top_pairs(df, args.top_n)
        for a, b, w in top_pairs:
            f.write(f"{a} <> {b}: {w}\n")

        f.write("\nField Interaction Strength (descending)\n")
        strengths = compute_field_strengths(df)
        for field, score in strengths:
            f.write(f"{field}: {score}\n")

        f.write("\nStrongest Inter-Cluster Links (max edge between clusters)\n")
        inter_links = compute_intercluster_links(df, clusters, args.top_n)
        for ca, cb, w in inter_links:
            f.write(f"{ca} <> {cb}: {w}\n")

        f.write("\nCluster Cohesion Metrics\n")
        for cid, members in clusters.items():
            sub = df.loc[members, members].to_numpy()
            n_members = len(members)
            if n_members > 1:
                intra_sum = float(sub[np.triu_indices(n_members, k=1)].sum())
                denom = n_members * (n_members - 1) / 2.0
                density = intra_sum / denom if denom else 0.0
                f.write(f"Cluster {cid}: size={n_members}, intra_sum={intra_sum}, density={density}\n")
            else:
                f.write(f"Cluster {cid}: size=1, intra_sum=0.0, density=0.0\n")

        f.write("\nPer-Field Top Partners\n")
        for field in df.columns:
            row = df.loc[field]
            partners = [(p, float(w)) for p, w in row.items() if p != field and w > 0]
            partners.sort(key=lambda x: x[1], reverse=True)
            topk = partners[: args.per_field_top_k]
            if topk:
                parts = ", ".join([f"{p} ({w})" for p, w in topk])
            else:
                parts = "None"
            f.write(f"{field}: {parts}\n")

    if args.json_out:
        json_payload: Dict[str, Any] = {}
        json_payload["reordered_fields"] = reordered_fields
        json_payload["clusters"] = [
            {
                "id": int(cid),
                "members": members,
                "size": len(members),
            }
            for cid, members in clusters.items()
        ]
        json_payload["top_pairs"] = [
            {"a": a, "b": b, "weight": w} for a, b, w in top_pairs
        ]
        json_payload["field_strength"] = [
            {"field": f, "strength": s} for f, s in strengths
        ]
        json_payload["intercluster_links"] = [
            {"cluster_a": ca, "cluster_b": cb, "max_weight": w} for ca, cb, w in inter_links
        ]
        cohesion: List[Dict[str, Any]] = []
        for cid, members in clusters.items():
            sub = df.loc[members, members].to_numpy()
            n_members = len(members)
            if n_members > 1:
                intra_sum = float(sub[np.triu_indices(n_members, k=1)].sum())
                denom = n_members * (n_members - 1) / 2.0
                density = intra_sum / denom if denom else 0.0
            else:
                intra_sum = 0.0
                density = 0.0
            cohesion.append({
                "cluster_id": int(cid),
                "size": n_members,
                "intra_sum": intra_sum,
                "density": density,
            })
        json_payload["cluster_cohesion"] = cohesion

        per_field_top: Dict[str, List[Dict[str, Any]]] = {}
        for field in df.columns:
            row = df.loc[field]
            partners = [(p, float(w)) for p, w in row.items() if p != field and w > 0]
            partners.sort(key=lambda x: x[1], reverse=True)
            topk = partners[: args.per_field_top_k]
            per_field_top[field] = [{"partner": p, "weight": w} for p, w in topk]
        json_payload["per_field_top_partners"] = per_field_top

        vals = df.to_numpy()
        num_fields = int(vals.shape[0])
        num_edges = int((vals > 0).sum() // 2)
        total_weight = float(vals.sum() / 2.0)
        max_edges = num_fields * (num_fields - 1) / 2.0
        sparsity = 1.0 - (num_edges / max_edges if max_edges else 0.0)
        json_payload["graph_stats"] = {
            "num_fields": num_fields,
            "num_edges_nonzero": num_edges,
            "total_weight": total_weight,
            "sparsity": sparsity,
        }

        with open(args.json_out, "w") as jf:
            json.dump(json_payload, jf, indent=2)

    print(f"Insights written to {args.output_file}")
    if args.json_out:
        print(f"JSON insights written to {args.json_out}")


if __name__ == "__main__":
    main()

