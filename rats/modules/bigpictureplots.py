from rats.modules import ratparser
import numpy as np
import pandas as pd
import plotly_express as px


def decimate_bp_plot(df):
    first = df.drop_duplicates(subset='function', keep='first')
    last = df.drop_duplicates(subset='function', keep='last')
    return pd.concat([first,last])


# function to run on initial upload
def bigpictureplot(df, decimate=False, timescale=1000000):
    df = df[['function', 'packet', 'llc', 'anomalous', 'time', 'board']]
    df.drop_duplicates(subset=['llc', 'anomalous'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.loc[:, 'state'] = np.where(df['anomalous'] == 0, 'GOOD', 'ERRORS')
    df.loc[:, 'timescale'] = timescale
    df.loc[:, 'time'] = df['time'] / df['timescale']
    title = df['board'].astype('str').unique()[0]

    if decimate:
        df = df.groupby('state').apply(decimate_bp_plot)
        fig = px.scatter(df, x='time', y='function', color='function', hover_data=['llc'], title=title, render_mode='webgl',
                      facet_row='state',
                         template='simple_white')
        fig.update_traces(mode='lines+markers')

    else:
        fig = px.scatter(df, x='time', y='function', color='state', hover_data=['llc'], title=title, render_mode='webgl')

    fig.update_layout(showlegend=False)
    fig.update_traces(marker=dict(size=12))
    fig.update_xaxes(showgrid=True)
    fig.update_yaxes(showgrid=True)

    return fig

def test_case(absolutepath):
    import pickle
    testclass = ratparser.RatParse(absolutepath)
    df = testclass.dataframe
    fig = bigpictureplot(df,decimate=True)

    import numpy as np
    import pandas as pd
    # np.random.seed(1)
    #
    # N = 1000000
    #
    # df = pd.DataFrame(dict(x=np.random.randn(N),
    #                        y=np.random.randn(N)))
    #
    # fig = px.scatter(df, x="x", y="y", render_mode='webgl')
    #
    # fig.update_traces(marker_line=dict(width=1, color='DarkSlateGray'))
    fig.write_html('test2.html')

# test_case('/users/steve/documents/workwaters/RATS simulation 1587681937.txt')
# test_case('/users/steve/documents/workwaters/5.txt')
