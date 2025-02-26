import plotly.graph_objects as go
import numpy as np
import pandas as pd
import dashtrade
import datetime
import asyncio

async def main():
    # --- Data Generation ---
    np.random.seed(42)
    n = 500
    symbol = 'BTC/USD'
    start_timestamp = datetime.datetime(2025, 1, 1)

    imbalance_theta = 1500000
    # imbalance_theta = 5000000
    data = await dashtrade.load_data_from_db(symbol, start_timestamp)
    # reverse the data
    print(type(data)) # list of dicts
    dib_bars = dashtrade.create_dollar_imbalance_bars(data, imbalance_theta)
    # dib bars is a dataframe
    # reverse the data

    # select the last 200 bars
    # print(f"Num bars: {dib_bars.shape[0]}")
    # dib_bars = dib_bars.iloc[-n:]
    # create rolling volatility feature
    dib_bars['volatility'] = dib_bars['close'].rolling(window=10).std()
    # create time_duration feature
    dib_bars['time_duration'] = dib_bars['end_time'] - dib_bars['end_time'].shift(1)
    dib_bars['time_duration_norm'] = dib_bars['time_duration'] / dib_bars['time_duration'].max()
    dib_bars['num_trades_norm'] = dib_bars['num_trades'] / dib_bars['num_trades'].max()
    dib_bars['close_norm'] = dib_bars['close'] / dib_bars['close'].max()
    dib_bars['close_pct_change'] = dib_bars['close'].pct_change()
    dib_bars['close_pct_change_norm'] = dib_bars['close_pct_change'] / dib_bars['close_pct_change'].max()
    dib_bars['close_pct_change_norm'] = (dib_bars['close_pct_change_norm'] - dib_bars['close_pct_change_norm'].min())
    dib_bars['close_pct_change_norm'] = dib_bars['close_pct_change_norm'] / dib_bars['close_pct_change_norm'].max()
    dib_bars['next_close_pct_change_norm'] = dib_bars['close_pct_change_norm'].shift(-1)
    print(f'Num trades norm max/min: {dib_bars["num_trades_norm"].max()} / {dib_bars["num_trades_norm"].min()}')
    dib_bars['volume_norm'] = dib_bars['volume'] / dib_bars['volume'].max()
    dib_bars['volatility_norm'] = dib_bars['volatility'] / dib_bars['volatility'].max()
    # shape is a categorical feature, if close is higher than open, shape is A, lower is C and equal is B
    dib_bars['shape'] = np.where(dib_bars['close'] > dib_bars['open'], 'Up', np.where(dib_bars['close'] < dib_bars['open'], 'Down', 'Equal'))
    print(f"Features: {dib_bars.columns}")
    print(f"Num rows: {dib_bars.shape[0]}")
    dib_bars['int_timestamp'] = dib_bars['end_time'].astype(np.int64)
    dib_bars['rel_int_timestamp'] = dib_bars['int_timestamp'] - dib_bars['int_timestamp'].min()
    dib_bars = dib_bars.dropna()

    data = pd.DataFrame({
        # x is the end_time of the bar
        'x': dib_bars['num_trades_norm'],
        'y': dib_bars['next_close_pct_change_norm'],
        'z': dib_bars['volatility_norm'],
        'color_dim': dib_bars['volume_norm'],
        'size_dim': dib_bars['time_duration_norm'] * 100,
        'shape_dim': dib_bars['shape']
    })
    for i in range(0, dib_bars.shape[0]):
        # build a matrix of the data in the other dimensions
        # then find the determinate of the matrix
        # this will be the x dimension
        x = np.array([[dib_bars['num_trades_norm'].iloc[i], dib_bars['volume_norm'].iloc[i]], [dib_bars['volume_norm'].iloc[i], dib_bars['volatility_norm'].iloc[i]]])
        data['x'].iloc[i] = np.linalg.det(x)


    # reverse the data
    # data = data.iloc[::-1]

    # --- Mapping Categories to Plotly Symbols ---
    symbol_map = {
        'Up': 'circle',
        'Equal': 'square',
        'Down': 'diamond',
    }
    # Apply the mapping to create a new column with Plotly-compatible symbols
    data['plotly_symbol'] = data['shape_dim'].map(symbol_map)

    # --- Plotly 3D Scatter Plot ---

    fig = go.Figure(data=[go.Scatter3d(
        x=data['x'],
        y=data['y'],
        z=data['z'],
        mode='markers',
        marker=dict(
            size=data['size_dim'],
            color=data['color_dim'],
            colorscale='Viridis',
            colorbar=dict(title='Color Dimension'),
            opacity=0.8,
            symbol=data['plotly_symbol'],  # Use the mapped symbols
        ),
        # --- Add customdata and hovertemplate ---
        customdata=data[['shape_dim']],  # Include original category for hover
        hovertemplate=(
            "X: %{x:.2f}<br>"
            "Y: %{y:.2f}<br>"
            "Z: %{z:.2f}<br>"
            "Color: %{marker.color:.2f}<br>"
            "Size: %{marker.size:.2f}<br>"
            "Shape Category: %{customdata[0]}<extra></extra>"  # Display original category
        )
    )
    ])

    # --- Layout and Customization ---
    fig.update_layout(
        title='Interactive 6D Scatter Plot (Plotly)',
        scene=dict(
            xaxis_title='Determinant',
            yaxis_title='Next Close % Change',
            zaxis_title='Volatility'
        ),
        margin=dict(l=0, r=0, b=0, t=50),
        showlegend=True,  # Explicitly enable the legend
        legend=dict(
        title='Shape Dimension',
        x=1,
        y=1,
        itemsizing='constant'
        )
    )

    # --- Manual Legend Creation ---
    # Plotly doesn't create a legend for the 'symbol' attribute in 3D plots.
    #  So we create it by adding traces for each symbol, but make them
    # invisible in the 3D plot itself (size=0), so they only appear in the legend.
    for category, symbol in symbol_map.items():
        fig.add_trace(go.Scatter3d(
            x=[None],  # Invisible point
            y=[None],
            z=[None],
            mode='markers',
            marker=dict(size=10, symbol=symbol),  # Visible in legend with size 10
            name=category,  # Show category in legend
            showlegend=True
            ))

    # --- Save as HTML ---
    fig.write_html("6d_plotly_plot.html")
    print("Plot saved as 6d_plotly_plot.html")

if __name__ == '__main__':
    asyncio.run(main())
