import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib as mpl
import numpy as np
import prettyplotlib as ppl


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

lines = {
    'tpch': '-',
    'sdss': '-',
    'sqlshare': '-'
}


font_scale = 1.5


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


def load_data(metric, wls=workloads):
    data = {}
    for workload in wls:
        with open(metric + '/' + workload + '.csv') as f:
            data[workload] = np.recfromcsv(f)
    return data


def num_ops():
    d = load_data("num_physops")

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in workloads:
        c = d[w]['count'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['number'], np.cumsum(c),
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 1)
    axes.set_xlim(0, 300)

    axes.yaxis.set_major_formatter(formatter)

    axes.set_xlabel('Number of physical operators')
    axes.set_ylabel('% of queries')

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_num_physops_cdf.pdf', format='pdf')
    plt.show()


def num_dist_ops():
    d = load_data("num_dist_physops")

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in workloads:
        c = d[w]['count'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['number'], np.cumsum(c),
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 1)
    axes.set_xlim(0, 12)

    axes.yaxis.set_major_formatter(formatter)

    axes.set_xlabel('Number of distinct physical operators')
    axes.set_ylabel('% of queries')

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_num_dist_physops_cdf.pdf', format='pdf')
    plt.show()



def query_length():
    d = load_data("query_length")

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in workloads:
        c = d[w]['count'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['char_length'], np.cumsum(c),
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 1)
    axes.set_xlim(0, 1500)

    axes.yaxis.set_major_formatter(formatter)

    axes.set_xlabel('Query length in characters')
    axes.set_ylabel('% of queries')

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_length_cdf.pdf', format='pdf')
    plt.show()


def table_touch():
    d = load_data("table_touch")

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in workloads:
        c = d[w]['count'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['number'], np.cumsum(c),
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 1)
    axes.set_xlim(0, 10)

    axes.yaxis.set_major_formatter(formatter)

    axes.set_xlabel('Table touch')
    axes.set_ylabel('% of queries')

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_touch_cdf.pdf', format='pdf')
    plt.show()


def column_touch():
    d = load_data("column_touch")

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in workloads:
        c = d[w]['count'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['number'], np.cumsum(c),
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 1)
    axes.set_xlim(0, 400)

    axes.yaxis.set_major_formatter(formatter)

    axes.set_xlabel('Column touch')
    axes.set_ylabel('% of queries')

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_column_touch_cdf.pdf', format='pdf')
    plt.show()


def runtime():
    ws = ["sdss", "sqlshare"]
    d = load_data("runtime", ws)

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in ws:
        c = d[w]['count'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['runtime'], np.cumsum(c),
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 1)
    axes.set_xlim(-0.005, 2)

    axes.yaxis.set_major_formatter(formatter)

    axes.set_xlabel('Runtime')
    axes.set_ylabel('% of queries')

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_runtime_cdf.pdf', format='pdf')
    plt.show()


def ops():
    for w in workloads:
        data = load_data("physops", [w])[w]

        sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
        # sns.set_style("whitegrid")

        fig, ax = plt.subplots(1, figsize=(8, 4))

        data.sort(order='count')

        c = data['count'].astype(float)
        c /= sum(c)
        c *= 100
        ypos = np.arange(len(data['phys_operator']))
        ppl.barh(ax, ypos, c, yticklabels=data['phys_operator'], grid='x', annotate=True, color=colors[w])

        #ax.set_ylabel('Physical operator')
        ax.set_xlabel('% of queries')

        ax.xaxis.grid(True)
        ax.yaxis.grid(False)

        #plt.subplots_adjust(bottom=.2, left=.3, right=.99, top=.9, hspace=.35)

        fig.tight_layout(rect=[0.03, 0, 1, 1])
        fig.text(0.02, 0.55, 'Physical operator', rotation=90, va='center')

        plt.savefig('plot_ops_%s.pdf' % w, format='pdf')
        plt.show()


def new_tables():
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    fig, ax = plt.subplots(1)

    with open('../results/sdss/query_number_num_new_tables.csv') as f:
        data = np.recfromcsv(f)
    c = data['num_new_tables'].astype(float)
    c /= sum(c)
    q = data['query_number'].astype(float)
    q /= q[-1]
    ax.plot(q, np.cumsum(c), label="SDSS", color=colors['sdss'], linewidth=2, drawstyle='steps-post')
    ax.scatter(q[0: -1], np.cumsum(c)[0: -1], color=colors['sdss'], marker="o", s=50, alpha=.7)

    with open('../results/tpch/query_number_num_new_tables.csv') as f:
        data = np.recfromcsv(f)
    c = data['num_new_tables'].astype(float)
    c /= sum(c)
    q = data['query_number'].astype(float)
    q /= q[-1]
    ax.plot(q, np.cumsum(c), label="TPC-H", color=colors['tpch'], linewidth=2, drawstyle='steps-post')
    ax.scatter(q[0: -1], np.cumsum(c)[0: -1], color=colors['tpch'], marker="o", s=50, alpha=.7)

    sns.rugplot([0.1, 0.2, 10, 100], ax=ax)

    with open('../results/sqlshare/table_coverage.csv') as f:
        data = np.recfromcsv(f)
    c = data['tables'].astype(float)
    c /= c[-1]
    q = data['query_id'].astype(float)
    q /= q[-1]
    ax.plot(q, c, label="SQLShare", color=colors['sqlshare'], linewidth=2, drawstyle='steps-post')
    # ax.scatter(q[0: -1], c[0: -1], color=colors['sqlshare'], marker="o", s=20, alpha=.01)

    ax.yaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_formatter(formatter)

    ax.set_xlabel('% of queries')
    ax.set_ylabel('% of newly used table')

    ax.set_ylim(0, 1.01)
    ax.set_xlim(-0.01, 1)

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('table_coverage.pdf', format='pdf')
    plt.show()

if __name__ == '__main__':
    # ops()
    # num_ops()
    # num_dist_ops()
    # query_length()
    # table_touch()
    column_touch()
    # runtime()
    # new_tables()
