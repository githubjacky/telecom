from bokeh.io import export_svgs
from bokeh.plotting import figure
from bokeh.palettes import magma
from bokeh.transform import cumsum
import holoviews as hv
from math import pi


def bokeh_output(plot, output_path: str, format: str = 'svg') -> None:
    match format:
        case "svg":
            plot_ = hv.render(plot)
            plot_.output_backend = format
            export_svgs(plot_, filename=output_path)
            # hv.save(plot_, output_path, fmt=format)
        case "html":
            renderer = hv.renderer('bokeh')
            renderer.save(plot, output_path)


def create_pie(df_pie):
    df_pie['ratio'] = df_pie['value']/df_pie['value'].sum()
    df_pie['angle'] = df_pie['ratio'] * 2*pi
    df_pie['color'] = magma(len(df_pie))

    p = figure(height=350, title="Pie Chart", toolbar_location=None,
               tools="hover", tooltips="@phone_brand: @ratio", x_range=(-0.5, 1.0))

    p.wedge(x=0, y=1, radius=0.4,
            start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
            line_color="white", fill_color='color', source=df_pie.to_pandas())

    p.axis.axis_label = None
    p.axis.visible = False
    p.grid.grid_line_color = None

    return p


def plot_rank_cate(df_node_attr, col: str, most: int = 15):
    if y is not None and y_cate is not None:
        df_ = (
            df[df[y] == y_cate]
            .groupby(x)
            .size()
            .reset_index(name='value')
            .copy()
        )
    else:
        df_ = df.groupby(x).size().reset_index(name='value').copy()

    df_['ratio'] = df_['value'] / df_['value'].sum()
    plot = (
        df_
        .sort_values('ratio', ascending=False)
        .iloc[:most]
        .hvplot
        .bar(x, 'ratio')
    )

    return plot