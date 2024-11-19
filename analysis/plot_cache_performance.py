# System imports
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_data(csv_file, title, reads=True):
    # Get output path
    out_filename = os.path.splitext(csv_file)[0] + '_figure.png'

    # Read the file into a data frame
    df = pd.read_csv(csv_file)

    # Extract class labels
    class_labels = df[df.columns[0]]

    # Extract values
    x = df.columns[1:].astype(int)
    y = df.iloc[0:,1:].to_numpy()

    # Set up graphing options
    sns.set_theme(style='darkgrid', palette='muted')
    fig = plt.figure(figsize=(10, 6))
    plt.xscale('log')
    plt.yscale('log')

    # Plot data
    for i in range(y.shape[0]):
        plt.plot(x, y[i], label = class_labels[i], marker='o')

    # Add plot descriptors
    plt.title(title, fontsize=16)
    plt.xlabel('Number of {0}'.format('reads' if reads else 'writes'))
    plt.ylabel('Time (s)')

    # Add legend
    plt.legend(title='Cache Policy')

    # Set additional style options
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save to file
    fig.savefig(out_filename, dpi=100)
