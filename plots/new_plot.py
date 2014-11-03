import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib as mpl


workloads = ["tpch", "sdss", "sqlshare"]
labels = {
    'tpch': "TPC-H",
    'sdss': "SDSS",
    'sqlshare': "SQLShare"
}

cs = b, g, r = sns.color_palette("deep", 3)
colors = {
    'tpch': b,
    'sdss': g,
    'sqlshare': r
}


def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    return str(int(100 * y))

    # The percent symbol needs escaping in latex
    if mpl.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    else:
        return s + '%'


formatter = FuncFormatter(to_percent)


def load_data(metric):
    data = {}
    for workload in workloads:
        with open(metric + '/' + workload + '.csv') as f:
            data[workload] = np.recfromcsv(f)
    return data


def query_length():
    d = load_data("table_touch")

    sns.set_context("paper", font_scale=1.3)
    sns.set_style("whitegrid")

    f, axes = plt.subplots(1, 2, figsize=(7, 4), sharey=True)

    sns.set(style="white", palette="muted")
    sns.despine(left=True)

    a = {
        'tpch': 0,
        'sdss': 0,
        'sqlshare': 1
    }
    for w in workloads:
        sns.kdeplot(d[w], cumulative=True, label=labels[w],
                    bw=1, ax=axes[a[w]], color=colors[w])

    axes[0].set_xlim(0, 10)
    axes[1].set_xlim(0, 400)

    axes[0].yaxis.set_major_formatter(formatter)

    axes[0].set_xlabel('Table touch')
    axes[1].set_xlabel('Table touch')
    axes[0].set_ylabel('% of queries')

    # f.text(0.5, 0.02, "Table touch", ha='center')

    plt.tight_layout()

    f.savefig('plot_touch_cdf.pdf', format='pdf', transparent=True)
    plt.show()


if __name__ == '__main__':
    query_length()
