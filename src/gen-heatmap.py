import pandas as pd
import plotly.express as px
import argparse

def main():
    parser = argparse.ArgumentParser(description="Generate a heatmap based on percentages relative to the highest value.")
    parser.add_argument("input_file", help="Path to the input CSV file containing the adjacency matrix.")
    parser.add_argument("output_file", help="Path to save the output HTML file.")
    args = parser.parse_args()

    csv_file = args.input_file
    output_file = args.output_file

    try:
        df = pd.read_csv(csv_file, index_col=0)
    except FileNotFoundError:
        print(f"Error: The file '{csv_file}' was not found.")
        return 1

    max_value = df.max().max()
    df_percentage = (df / max_value) * 100

    color_scale = [
        (0.00, "#e5e5e5"),  # 0–1%: Very light gray
        (0.01, "#d9e2dc"),  # 2–3%: Light muted green
        (0.03, "#c2d8cf"),  # 4–5%: Soft teal
        (0.05, "#a8cbc0"),  # 6–7%: Muted aqua
        (0.07, "#85b8af"),  # 8–9%: Soft green
        (0.09, "#6ba99f"),  # 10–11%: Matte green
        (0.11, "#4a8b80"),  # 12–13%: Muted teal
        (0.13, "#34695f"),  # 14–15%: Dark muted green
        (0.15, "#2e5551"),  # 16–17%: Dark matte forest green
        (0.17, "#243d3f"),  # 18–19%: Very dark green
        (0.19, "#345b8a"),  # 20–21%: Soft blue-gray
        (0.21, "#3e6799"),  # 22–23%: Muted navy blue
        (0.23, "#4873a8"),  # 24–25%: Deep blue
        (0.25, "#507bb7"),  # 26–27%: Rich blue
        (0.27, "#5983c6"),  # 28–29%: Medium blue
        (0.29, "#6179d5"),  # 30–31%: Muted royal blue
        (0.31, "#6a8ee4"),  # 32–33%: Bright navy
        (0.33, "#728df3"),  # 34–35%: Soft indigo
        (0.35, "#7b7dfa"),  # 36–37%: Light indigo
        (0.37, "#8375ff"),  # 38–39%: Muted violet
        (0.39, "#8b6aff"),  # 40–41%: Rich violet
        (0.41, "#9460ff"),  # 42–43%: Deep violet
        (0.43, "#9b55ff"),  # 44–45%: Soft purple
        (0.45, "#a44aff"),  # 46–47%: Rich purple
        (0.47, "#ac3fff"),  # 48–49%: Matte purple
        (0.49, "#b534ff"),  # 50–51%: Bold purple
        (0.51, "#c229f8"),  # 52–53%: Deep pink-purple
        (0.53, "#d71fe3"),  # 54–55%: Vibrant pink
        (0.55, "#e015c9"),  # 56–57%: Muted magenta
        (0.57, "#e60bb0"),  # 58–59%: Deep magenta
        (0.59, "#ec0098"),  # 60–61%: Bright magenta
        (0.61, "#f00081"),  # 62–63%: Matte fuchsia
        (0.63, "#f40069"),  # 64–65%: Deep muted red
        (0.65, "#f80052"),  # 66–67%: Rich red
        (0.67, "#fc003b"),  # 68–69%: Vibrant red
        (0.69, "#ff0024"),  # 70–71%: Bold red
        (0.71, "#ff140d"),  # 72–73%: Bright crimson
        (0.73, "#ff2900"),  # 74–75%: Deep crimson
        (0.75, "#ff3f00"),  # 76–77%: Rich orange-red
        (0.77, "#ff5500"),  # 78–79%: Deep orange
        (0.79, "#ff6b00"),  # 80–81%: Vibrant orange
        (0.81, "#ff8100"),  # 82–83%: Bright orange
        (0.83, "#ff9700"),  # 84–85%: Soft orange
        (0.85, "#ffad00"),  # 86–87%: Muted amber
        (0.87, "#ffc300"),  # 88–89%: Rich gold
        (0.89, "#ffdb00"),  # 90–91%: Vibrant yellow
        (0.91, "#ffe000"),  # 92–93%: Bright yellow
        (0.93, "#ffe600"),  # 94–95%: Soft yellow
        (0.95, "#ffec00"),  # 96–97%: Pale yellow
        (0.97, "#fff200"),  # 98–99%: Very light yellow
        (1.00, "#fff700"),  # 100%: Brightest yellow
    ]

    fig = px.imshow(
        df_percentage,
        text_auto=True,
        aspect="auto",
        color_continuous_scale=color_scale,
        labels={"color": "Percentage"},
        title="Heatmap Based on Percentage Relative to Max Value..",
        zmin=0,
        zmax=100,
    )

    fig.update_layout(
        xaxis=dict(title="Columns"),
        yaxis=dict(title="Rows"),
        title_x=0.5,
        coloraxis_colorbar=dict(title="Percentage", ticks="outside"),
    )

    fig.write_html(output_file)

    print(f"Heatmap saved as {output_file}")
    return 0

if __name__ == "__main__":
    exit(main())

