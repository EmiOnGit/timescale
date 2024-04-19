import plotly.graph_objects as go


class TsView:
    def __init__(self):
        pass

    def draw(self, ts1, state):
        corr_df = state.get_corr()
        ts2_trans = state.cached
        fig = go.Figure()
        fig.add_scatter(x=ts2_trans.df["timestamp"], y=ts2_trans.df["value-0"])
        fig.add_scatter(x=ts1.df["timestamp"], y=ts1.df["value-0"])
        fig.add_bar(x=corr_df["timestamp"], y=corr_df["corr"], name="correlation")
        return fig
