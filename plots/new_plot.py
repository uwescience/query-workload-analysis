from datetime import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib as mpl
import numpy as np
import prettyplotlib as ppl
import matplotlib.dates as mdates

workloads = ["tpch","sdss", "sqlshare"]
labels = {
    'tpch' : "TPC-H",
    'sdss': "SDSS",
    'sqlshare': "SQLShare"
}

b, g, r = sns.color_palette("deep", 3)
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


font_scale = 1.7


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
        with open('../results/' + metric + '/' + workload + '.csv') as f:
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
    # axes.set_xlim(0, 300)
    axes.set_xscale('log')

    axes.yaxis.set_major_formatter(formatter)

    plt.title("CDF of number of operators per query")
    axes.set_xlabel('Number of physical operators')
    axes.set_ylabel('% of queries')

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_num_physops_cdf.eps', format='eps')
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
    # axes.set_xlim(0, 100)

    axes.yaxis.set_major_formatter(formatter)

    plt.title("CDF of number of distinct operators per query")
    axes.set_xlabel('Number of distinct physical operators')
    axes.set_ylabel('% of queries')

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_num_dist_physops_cdf.eps', format='eps')
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

    plt.title("CDF of query length")
    axes.set_xlabel('Query length in characters')
    axes.set_ylabel('% of queries')

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_length_cdf.eps', format='eps')
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

    plt.title("CDF of table touch")
    axes.set_xlabel('Table touch')
    axes.set_ylabel('% of queries')

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_touch_cdf.eps', format='eps')
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
    axes.set_xlim(0, 100)

    axes.yaxis.set_major_formatter(formatter)

    plt.title("CDF of column touch")
    axes.set_xlabel('Column touch')
    axes.set_ylabel('% of queries')

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_column_touch_cdf.eps', format='eps')
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

    plt.title("CDF of query runtime")
    axes.set_xlabel('Runtime')
    axes.set_ylabel('% of queries')

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_runtime_cdf.eps', format='eps')
    plt.show()

def complexity():
    w = 'sqlshare'
    owners = ['','billhowe', 'sr320@washington.edu', 'isaphan@washington.edu', 'emmats@washington.edu', 'koesterj@washington.edu', 'micaela@washington.edu',
              'bifxcore@gmail.com', 'sism06@comcast.net', 'koenigk92@gmail.com', 'rkodner', 'erin.s1964@gmail.com', 'fridayharboroceanographers@gmail.com']
    for owner in owners:
        with open('../results/sqlshare/'+owner+'complexity_by_time.csv') as f:
            d = np.recfromcsv(f)

        sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
        # sns.set_style("whitegrid")

        plt.plot(d['query_id']*100.0/max(d['query_id']), d['complexity'].astype(float), linewidth=0.5, color=b, ls=lines[w])

        axes = plt.gca()

        # axes.set_ylim(0, 15)
        # axes.set_xlim(-0.005, max(d['query_id'])+10)

        # axes.yaxis.set_major_formatter(formatter)

        plt.title('Query complexity over time for a SQLShare user')
        axes.set_xlabel('% Queries')
        axes.set_ylabel('Query Complexity')

        axes.title.set_position((axes.title._x, 1.04))

        plt.legend(loc=4)
        plt.tight_layout()

        plt.savefig(owner + 'plot_complexity_cdf.eps', format='eps')
        plt.savefig(owner + 'plot_complexity_cdf.pdf', format='pdf')
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

        plt.title("Types of operators in {}".format(labels[w]))
        #ax.set_ylabel('Physical operator')
        ax.set_xlabel('% of queries')

        ax.xaxis.grid(True)
        ax.yaxis.grid(False)

        #plt.subplots_adjust(bottom=.2, left=.3, right=.99, top=.9, hspace=.35)

        ax.title.set_position((ax.title._x, 1.04))
        fig.tight_layout(rect=[0.03, 0, 1, 1])
        fig.text(0.02, 0.55, 'Physical operator', rotation=90, va='center')

        plt.savefig('plot_ops_%s.eps' % w, format='eps')
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
    # ax.scatter(q[0: -1], np.cumsum(c)[0: -1], color=colors['sdss'], marker="o", s=50, alpha=.7)

    with open('../results/tpch/query_number_num_new_tables.csv') as f:
        data = np.recfromcsv(f)
    c = data['num_new_tables'].astype(float)
    c /= sum(c)
    q = data['query_number'].astype(float)
    q /= q[-1]
    ax.plot(q, np.cumsum(c), label="TPC-H", color=colors['tpch'], linewidth=2, drawstyle='steps-post')
    # ax.scatter(q[0: -1], np.cumsum(c)[0: -1], color=colors['tpch'], marker="o", s=50, alpha=.7)

    # sns.rugplot([0.1, 0.2, 10, 100], ax=ax)

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

    plt.title("CDF of new tables")
    ax.set_xlabel('% of queries')
    ax.set_ylabel('% of newly used table')

    ax.set_ylim(0, 1.01)
    ax.set_xlim(-0.01, 1)

    ax.title.set_position((ax.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig('plot_table_coverage.eps', format='eps')
    plt.show()

def new_tables_for_users():
    owners = ['billhowe', 'sr320@washington.edu', 'isaphan@washington.edu', 'emmats@washington.edu', 'koesterj@washington.edu', 'micaela@washington.edu',
              'bifxcore@gmail.com', 'sism06@comcast.net', 'koenigk92@gmail.com', 'rkodner', 'erin.s1964@gmail.com', 'fridayharboroceanographers@gmail.com']
    for owner in owners:
        sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

        fig, ax = plt.subplots(1)

        with open('../results/sqlshare/'+owner+'table_coverage.csv') as f:
            data = np.recfromcsv(f)
        c = data['tables'].astype(float)
        c /= c[-1]
        q = data['query_id'].astype(float)
        q /= q[-1]
        ax.plot(q, c, color=b, linewidth=2, drawstyle='steps-post')
        # ax.scatter(q[0: -1], c[0: -1], color=colors['sqlshare'], marker="o", s=20, alpha=.01)

        ax.yaxis.set_major_formatter(formatter)
        ax.xaxis.set_major_formatter(formatter)

        plt.title("CDF of new tables for a SQLShare user")
        ax.set_xlabel('% of queries')
        ax.set_ylabel('% of newly used table')

        ax.set_ylim(0, 1.01)
        ax.set_xlim(-0.01, 1)

        ax.title.set_position((ax.title._x, 1.04))

        plt.legend(loc=4)
        plt.tight_layout()

        plt.savefig('plot_table_coverage_'+owner+'.eps', format='eps')
        plt.show()

def Q_vs_D():
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    fig, ax = plt.subplots(1)

    with open('../results/sqlshare/user_Q_D.csv') as f:
        data = np.recfromcsv(f)
    D = data['d'].astype(float)
    Q = data['q'].astype(float)
    ax.scatter(D, Q, color=b, marker="o", s=20, alpha=0.5)
    # ax.plot(Q, D, color=colors['sqlshare'], "0")
    # ax.scatter(q[0: -1], c[0: -1], color=colors['sqlshare'], marker="o", s=20, alpha=.01)

    ax.yaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_formatter(formatter)

    ax.set_ylim(0.5, 1500)
    ax.set_xlim(0.5, 800)


    ax.set_xscale('log')
    ax.set_yscale('log')

    plt.title("High Variety in SQLShare Workload")
    ax.set_xlabel('Distinct Datasets')
    ax.set_ylabel('Distinct Queries')

    plt.tight_layout()

    plt.savefig('plot_Q_D.eps', format='eps')
    plt.show()

def lifetime():
    owners = ['','billhowe', 'sr320@washington.edu', 'isaphan@washington.edu', 'emmats@washington.edu', 'koesterj@washington.edu', 'micaela@washington.edu',
              'bifxcore@gmail.com', 'sism06@comcast.net', 'koenigk92@gmail.com', 'rkodner', 'erin.s1964@gmail.com', 'fridayharboroceanographers@gmail.com']
    for owner in owners:
        sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

        fig, ax = plt.subplots(1)

        with open('../results/sqlshare/'+owner+'query_lifetime.csv') as f:
            data = np.recfromcsv(f)
        Lifetime = data['lifetime'].astype(float)
        query_id = data['query_id'].astype(float)
        ax.plot(query_id, Lifetime, color = b, marker = 'o', ls ='.', alpha = 0.3)


        plt.title("Lifetime of datasets (in days) for a SQLShare user")
        ax.set_xlabel('Dataset')
        ax.set_ylabel('Lifetime (in days)')

        plt.tight_layout()

        plt.savefig('plot_query_lifetime'+owner+'.eps', format='eps')
        plt.show()

def cumulative_q_t():
    owners = ['billhowe', 'sr320@washington.edu', 'isaphan@washington.edu', 'emmats@washington.edu', 'koesterj@washington.edu', 'micaela@washington.edu',
              'bifxcore@gmail.com', 'sism06@comcast.net', 'koenigk92@gmail.com', 'rkodner', 'erin.s1964@gmail.com', 'fridayharboroceanographers@gmail.com']
    years    = mdates.YearLocator()   # every year
    months   = mdates.MonthLocator()  # every month
    yearsFmt = mdates.DateFormatter('%Y')
    for owner in owners:
        sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

        fig, ax = plt.subplots(1)

        with open('../results/sqlshare/q_t_bytime/'+owner+'cummu_q_table_by_time.csv') as f:
            data = np.recfromcsv(f)
        q = data['q'].astype(float)
        t = data['t'].astype(float)
        timestamp = data['timestamp'].astype(str)
        hoursfromstart = []
        start = datetime.strptime(timestamp[0].strip(),"%m/%d/%Y %I:%M:%S %p")
        for i,ti in enumerate(timestamp):
            hoursfromstart.append((datetime.strptime(ti.strip(),"%m/%d/%Y %I:%M:%S %p") - start).days)
        ax.plot(hoursfromstart, q, color = b)
        for tl in ax.get_yticklabels():
            tl.set_color(b)
        ax2 = ax.twinx()

        ax2.plot(hoursfromstart, t, color = r)
        for tl in ax2.get_yticklabels():
            tl.set_color(r)
        # ax.format_xdata = mdates.DateFormatter("%m/%d/%Y %I:%M:%S %p")
        # ax.xaxis.set_major_locator(years)
        # ax.xaxis.set_major_formatter(yearsFmt)
        # ax.xaxis.set_minor_locator(months)
        ax2.set_ylim(0, 1.1*max(q))
        ax.set_ylim(0, 1.1*max(q))

        plt.title("Number of Queries and Datasets over time")
        ax.set_xlabel('Time (in days)')
        ax.set_ylabel('Cumulative Queries')
        ax2.set_ylabel('Cumulative Datasets')

        plt.tight_layout()

        plt.savefig('plot_q_t_'+owner+'.eps', format='eps')
        plt.show()

if __name__ == '__main__':
    # ops()
    # num_ops()
    # num_dist_ops()
    # query_length()
    # table_touch()
    # column_touch()
    # runtime()
    # new_tables()
    # new_tables_for_users()
    # complexity()
    Q_vs_D()
    # lifetime()
    # cumulative_q_t()
