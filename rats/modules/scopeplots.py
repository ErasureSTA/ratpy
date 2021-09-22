import rats.modules.ratparser as ratparser
import plotly_express as px
import pandas as pd


def scopeplot(df, llc=0, buffer=1, facet=False, timescale=1000000):
    start = llc - buffer
    end = llc + buffer

    df['llc'] = df['llc'].astype('int')
    df['function'] = df['function'].astype('int')
    df = df[(df['llc'] >= start) & (df['llc'] <= end)]
    df.reset_index(drop=True, inplace=True)
    df.loc[:, 'timescale'] = timescale
    df.loc[:, 'time'] = df['time'] / df['timescale']
    title = df['board'].astype('str').unique()[0]

    if facet:
        fig = px.line(df, x='time', y='data', color='edb', facet_row='edb', hover_data=['llc', 'function'], title=title,
                      template='simple_white')
        fig.update_yaxes(matches=None)
        fig.for_each_annotation(lambda a: a.update(text=''))
        fig.update_layout(showlegend=False)
    else:
        fig = px.line(df, x='time', y='data', color='edb', hover_data=['llc', 'function'], title=title,
                      template='simple_white')

    fig.update_xaxes(showgrid=True)
    fig.update_yaxes(showgrid=True)

    # make sure markers are there in case user wants a single MRM scan, which would just be a single datapoint per edb
    fig.update_traces(mode='markers+lines', marker=dict(size=4))

    return fig
