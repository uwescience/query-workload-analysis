from datetime import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.ticker import ScalarFormatter
import matplotlib as mpl
import numpy as np
import prettyplotlib as ppl
import matplotlib.dates as mdates
from scipy.interpolate import spline
import re
import math

from matplotlib import rc
rc('font',**{'family':'serif','serif':['Palatino']})
rc('text', usetex=True)

# workloads = ["tpch","sdss", "sqlshare"]
workloads = ["sdss", "sqlshare"]
labels = {
    # 'tpch' : "TPC-H",
    'sdss': "SDSS",
    'sqlshare': "SQLShare"
}

b, g, r = sns.color_palette("deep", 3)
colors = {
    # 'tpch': b,
    'sdss': g,
    'sqlshare': r
}

lines = {
    # 'tpch': '-',
    'sdss': '-',
    'sqlshare': '-'
}


font_scale = 1.7

root_path = '../2015-sqlshare-sigmod/figures/'

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
        d[w].sort(order='number')
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
    axes.set_ylabel('\% of queries')
    axes.xaxis.grid(False)
    axes.yaxis.grid(False)

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_num_physops_cdf.eps', format='eps')
    plt.show()


def num_dist_ops():
    d = load_data("num_dist_physops")


    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in workloads:
        d[w].sort(order='number')
        c = d[w]['count'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['number'], np.cumsum(c),
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 1.1)
    # axes.set_xlim(0, 100)

    axes.yaxis.set_major_formatter(formatter)

    plt.title("CDF of number of distinct operators per query")
    axes.set_xlabel('Number of distinct physical operators')
    axes.set_ylabel('\% of queries')

    axes.title.set_position((axes.title._x, 1.04))
    axes.xaxis.grid(False)
    axes.yaxis.grid(False)

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_num_dist_physops_cdf.eps', format='eps')
    plt.show()



def query_length():
    d = load_data("query_length")

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    # sns.set_style("whitegrid")

    for w in workloads:
        c = d[w]['c'].astype(float)
        c /= sum(c)
        plt.plot(d[w]['l'], np.cumsum(c)*100,
                 label=labels[w], color=colors[w], ls=lines[w])

    axes = plt.gca()

    axes.set_ylim(0, 105)
    # axes.set_xlim(0, 1500)

    axes.yaxis.set_major_formatter(formatter)

    plt.title("CDF of query length")
    axes.set_xlabel('Query length in characters')
    axes.set_ylabel('\% of queries')

    axes.title.set_position((axes.title._x, 1.04))
    axes.xaxis.grid(False)
    axes.yaxis.grid(False)
    axes.set_xscale('log')
    for axis in [axes.xaxis, axes.yaxis]:
        axis.set_major_formatter(ScalarFormatter())
    # axes.set_xscale('log')


    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_length_cdf.eps', format='eps')
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
    axes.set_ylabel('\% of queries')

    axes.title.set_position((axes.title._x, 1.04))
    axes.xaxis.grid(False)
    axes.yaxis.grid(False)

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_touch_cdf.eps', format='eps')
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
    axes.set_ylabel('\% of queries')
    axes.xaxis.grid(False)
    axes.yaxis.grid(False)

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_column_touch_cdf.eps', format='eps')
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
    axes.set_ylabel('\% of queries')
    axes.xaxis.grid(False)
    axes.yaxis.grid(False)

    axes.title.set_position((axes.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_runtime_cdf.eps', format='eps')
    plt.show()

def complexity():
    w = 'sqlshare'
    owners = [''] #TODO: add actual owner list
    for owner in owners:
        with open('../results/sqlshare/'+owner+'complexity_by_time.csv') as f:
            d = np.recfromcsv(f)

        sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
        # sns.set_style("whitegrid")
        x = d['query_id']*100.0/max(d['query_id'])
        y = d['complexity'].astype(float)

        xnew = np.linspace(x.min(),x.max(),100)

        power_smooth = spline(x,y,xnew)

        plt.plot(xnew, power_smooth, linewidth=0.5, color=b, ls=lines[w])
        axes = plt.gca()

        axes.set_ylim(0, max(d['complexity'])*1.2)
        # axes.set_xlim(-0.005, max(d['query_id'])+10)

        # axes.yaxis.set_major_formatter(formatter)

        plt.title('Query complexity over time for a SQLShare user')
        axes.set_xlabel('\% Queries')
        axes.set_ylabel('Query Complexity')

        axes.title.set_position((axes.title._x, 1.04))
        axes.xaxis.grid(False)
        axes.yaxis.grid(False)

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

        plt.title("Operator Frequency in {}".format(labels[w]))
        #ax.set_ylabel('Physical operator')
        ax.set_xlabel('\% of queries')

        ax.xaxis.grid(False)
        ax.yaxis.grid(False)

        #plt.subplots_adjust(bottom=.2, left=.3, right=.99, top=.9, hspace=.35)

        ax.title.set_position((ax.title._x, 1.04))
        fig.tight_layout(rect=[0.03, 0, 1, 1])
        fig.text(0.02, 0.55, 'Physical operator', rotation=90, va='center')

        plt.savefig(root_path + 'plot_ops_%s.eps' % w, format='eps')
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
    ax.set_xlabel('\% of queries')
    ax.set_ylabel('\% of newly used table')

    ax.set_ylim(0, 1.01)
    ax.set_xlim(-0.01, 1)

    ax.title.set_position((ax.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_table_coverage.eps', format='eps')
    plt.show()

def new_tables_for_users():
    owners = ['billhowe', 'sr320@washington.edu', 'isaphan@washington.edu', 'emmats@washington.edu', 'koesterj@washington.edu', 'micaela@washington.edu',
              'bifxcore@gmail.com', 'sism06@comcast.net', 'koenigk92@gmail.com', 'rkodner', 'erin.s1964@gmail.com', 'fridayharboroceanographers@gmail.com']

    fig, ax = plt.subplots(1)
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    for owner in owners:
        with open('../results/sqlshare/'+owner+'table_coverage.csv') as f:
            data = np.recfromcsv(f)
        c = data['tables'].astype(float)
        c /= c[-1]
        q = data['query_id'].astype(float)
        q /= q[-1]
        if owner == 'sr320@washington.edu':
            ax.plot(q, c, color=r, linewidth=2, drawstyle='steps-post')
        else:
            ax.plot(q, c, color='grey', linewidth=2, drawstyle='steps-post')

        # ax.scatter(q[0: -1], c[0: -1], color=colors['sqlshare'], marker="o", s=20, alpha=.01)

    ax.yaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_formatter(formatter)

    plt.title("Query coverage of uploaded data for 12 most active users")
    ax.set_xlabel('\% of queries')
    ax.set_ylabel('\% of newly used table')

    ax.set_ylim(0, 1.01)
    ax.set_xlim(-0.01, 1)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)

    ax.title.set_position((ax.title._x, 1.04))

    plt.legend(loc=4)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_table_coverage.eps', format='eps')
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

    plt.title("SQLShare Usage Patterns")
    ax.set_xlabel('Distinct Datasets')
    ax.set_ylabel('Distinct Queries')
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)


    plt.tight_layout()

    plt.savefig(root_path + 'plot_Q_D.eps', format='eps')
    plt.show()

def lifetime():
    owners = ['billhowe', 'sr320@washington.edu', 'isaphan@washington.edu', 'emmats@washington.edu', 'koesterj@washington.edu', 'micaela@washington.edu',
              'bifxcore@gmail.com', 'sism06@comcast.net', 'koenigk92@gmail.com', 'rkodner', 'erin.s1964@gmail.com', 'fridayharboroceanographers@gmail.com']
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    fig, ax = plt.subplots(1)

    for owner in owners:

        with open('../results/sqlshare/'+owner+'query_lifetime.csv') as f:
            data = np.recfromcsv(f)
        data.sort(order = 'lifetime')
        Lifetime = data['lifetime'].astype(float)
        query_id = np.arange(len(data['lifetime']))*100.0/len(data['lifetime'])
        print query_id
        # print query_id,Lifetime
        # Lifetime = Lifetime[::-1]
        # query_id = query_id[::-1]
        
        for i,l in enumerate(Lifetime):
            if l == 0:
                Lifetime[i] = 1

        if owner == 'sr320@washington.edu':
            ax.plot(query_id, Lifetime, color = r, marker = '.', ls ='-', alpha = 0.3)
        else:
            ax.plot(query_id, Lifetime, color = 'grey', marker = '.', ls ='-', alpha = 0.3)

    plt.title("Lifetime of datasets for 12 most active users")
    ax.set_xlabel('\% of datasets')
    ax.set_ylabel('Lifetime (in days)')
    ax.set_yscale('log')
    for axis in [ax.xaxis, ax.yaxis]:
        axis.set_major_formatter(ScalarFormatter())
    plt.tight_layout()
    ax.set_xlim(0, 102)

    ax.xaxis.grid(False)
    ax.yaxis.grid(False)

    plt.savefig(root_path + 'plot_query_lifetime.eps', format='eps')
    plt.show()

def viewdepth():
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    fig, ax = plt.subplots(1)

    with open('../results/sqlshare/view_depth.csv') as f:
        data = np.recfromcsv(f)
    data.sort(order='max_depth')

    depth = data['max_depth'].astype(float)
    users = data['user']
    # depth = depth[::-1]
    # users = users[::-1]
    ax.plot(range(len(users)), depth, color = b, marker = 'o', ls ='.', alpha = 0.3)

    plt.title("Max View Depth for top 100 users")
    ax.set_xlabel('User index')
    ax.set_ylabel('Maximum depth')
    ax.set_ylim(0, 1.1*max(depth))
    ax.set_xlim(-0.5, len(users)+ 1)
    plt.tight_layout()
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)

    plt.savefig(root_path + 'plot_query_viewdepth.eps', format='eps')
    plt.show()

def cumulative_q_t():
    owners = ['']
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
        ax.xaxis.grid(False)
        ax.yaxis.grid(False)

        plt.tight_layout()

        plt.savefig(root_path + 'plot_q_t_'+owner+'.eps', format='eps')
        plt.show()

def queries_per_table():
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    with open('../results/sqlshare/queries_per_table.csv') as f:
        data = np.recfromcsv(f)
    num_queries = data['num_queries'].astype(float)
    tables = data['table']
    p = re.compile(ur'^.*[A-F0-9]{5}$')

    logical_tables = []
    for t in tables:
        short_name = re.findall(p, t)
        if len(short_name) == 0:
            if t not in logical_tables:
                logical_tables.append(t)
        else:
            if short_name[0][0:-5] not in logical_tables:
                logical_tables.append(short_name[0][0:-5])
    num_queries_lt = []
    for lt in logical_tables:
        max_num_queries = 0
        for i,t in enumerate(tables):
            if lt in t:
                if num_queries[i] > max_num_queries:
                    max_num_queries = num_queries[i]
        num_queries_lt.append(max_num_queries)

    c = [0,0,0,0,0]
    print sorted(num_queries_lt)

    print len(num_queries_lt)

    print sum(num_queries_lt)/len(num_queries_lt)

    for num in num_queries_lt:
        if num == 1.0:
            c[0] += 1
        elif num == 2.0:
            c[1] += 1
        elif num == 3.0:
            c[2] += 1
        elif num == 4.0:
            c[3] += 1
        else:
            c[4] += 1


    fig, ax = plt.subplots(1, figsize=(8, 4))

    ypos = np.arange(len(c))
    ppl.barh(ax, ypos, c, yticklabels=['1', '2', '3', '4', '$>=5$'], grid='x', annotate=True, color=g)

    plt.title("Number of queries per table")
    ax.set_ylabel('Number of queries')
    ax.set_xlabel('Number of tables')

    ax.xaxis.grid(False)
    ax.yaxis.grid(False)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_queries_per_table.eps', format='eps')
    plt.show()

def query_entropy():
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    fig, ax = plt.subplots(1, figsize=(8, 4))

    data = [3, 0.3, 0.2]
    data_sql = [96.18, 63.07, 45.35]
    xpos = np.arange(3)
    # width = 0.2
    margin = 0.1
    width = (1.-3.*margin)/3

    ticklabels=['\% Q_dist_strings', '\% Q_dist_templates', '\% Q_dist_columns']
    
    rects1 = ax.bar(xpos, data, width, color = colors['sdss'], label = 'SDSS')
    rects2 = ax.bar(xpos+width, data_sql, width, color = colors['sqlshare'], label = 'SQLShare')

    # ppl.barh(ax, ypos, data, yticklabels=ticklabels, grid='x', annotate=True, color=g)

    plt.title("Workload Entropy")
    ax.set_ylabel('\% of queries')
    ax.set_xticks(xpos+width)
    ax.set_xticklabels(ticklabels)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)
    ax.set_ylim(0, 120)
    ax.set_xlim(-0.1, 2.6)
    plt.tight_layout()
    plt.legend(loc = 1)
    def autolabel(rects):
    # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%.2f'%height + '%',
                ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)
    plt.savefig(root_path + 'plot_query_entropy.eps', format='eps')
    plt.show()

def num_dist_ops_hist():
    d = load_data("num_dist_physops")
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    fig, ax = plt.subplots(1, figsize=(8, 4))

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    width = 0.2
    xpos = {}
    xpos['sdss'] = np.arange(3)
    xpos['sqlshare'] = np.arange(3) + width
    
    ticklabels=['$<4$', '4-8', '$>=8$']

    for w in workloads:
        count = d[w]['count'].astype(float)
        number = d[w]['number'].astype(float)
        c = [0,0,0]
        for i,num in enumerate(number):
            if num < 4:
                c[0] += count[i]
            elif num < 8:
                c[1] += count[i]
            else:
                c[2] += count[i]
        sumc = sum(c)
        c[0] = (c[0] * 100.0) / sumc
        c[1] = (c[1] * 100.0) / sumc
        c[2] = (c[2] * 100.0) / sumc
        ax.bar(xpos[w], c, width, color = colors[w], label = labels[w])


    plt.title("Distinct Operators in Queries")
    ax.set_xlabel('Number of Distinct Operators')
    ax.set_ylabel('\% of queries')

    ax.set_xticks(xpos['sdss']+width)
    ax.set_xticklabels(ticklabels)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)
    ax.set_ylim(0, 110)

    plt.legend(loc=1)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_num_dist_physops_cdf.eps', format='eps')
    plt.show()

def query_length_hist():
    d = load_data("query_length")

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})
    fig, ax = plt.subplots(1, figsize=(8, 4))

    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    width = 0.2
    xpos = {}
    xpos['sdss'] = np.arange(4)
    xpos['sqlshare'] = np.arange(4) + width
    
    ticklabels=['$<100$','100-500','500-1000','$>1000$']

    for w in workloads:
        count = d[w]['count'].astype(float)
        char_length = d[w]['char_length'].astype(float)
        c = [0,0,0,0]
        for i,length in enumerate(char_length):
            if length < 100:
                c[0] += count[i]
            elif length < 500:
                c[1] += count[i]
            elif length < 1000:
                c[2] += count[i]
            else:
                c[3] += count[i]
        sumc = sum(c)
        c[0] = (c[0] * 100.0) / sumc
        c[1] = (c[1] * 100.0) / sumc
        c[2] = (c[2] * 100.0) / sumc
        c[3] = (c[3] * 100.0) / sumc
        ax.bar(xpos[w], c, width, color = colors[w], label = labels[w])


    plt.title("Query Length")

    ax.set_xticks(xpos['sdss']+width)
    ax.set_xticklabels(ticklabels)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)

    
    ax.set_xlabel('Query length in characters')
    ax.set_ylabel('\% of queries')
    ax.set_ylim(0, 110)

    plt.legend(loc=1)
    plt.tight_layout()

    plt.savefig(root_path + 'plot_length_cdf.eps', format='eps')
    plt.show()

def viewdepth_hist():
    sns.set_context("paper", font_scale=font_scale, rc={"lines.linewidth": 2.5})

    fig, ax = plt.subplots(1)

    with open('../results/sqlshare/view_depth_new.csv') as f:
        data = np.recfromcsv(f)
    
    depth = data['max_depth'].astype(float)
    
    c = [0,0,0]
    xpos = np.arange(3)

    ticklabels=['1-3','4-8','8+']

    for d in depth:
        if d < 3:
            c[0] += 1
        elif d < 8:
            c[1] += 1
        else:
            c[2] += 1
    ax.bar(xpos + 2.5, c, 0.3, color = g, align='center')


    plt.title("Max View Depth for top 100 users")

    ax.set_xticks(xpos + 2.5)
    ax.set_xticklabels(ticklabels)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)
    ax.set_ylim(0, 100)

    ax.set_xlabel('View Depth')
    ax.set_ylabel('Number of users')

    plt.tight_layout()

    plt.savefig(root_path + 'plot_query_viewdepth.eps', format='eps')
    plt.show()

if __name__ == '__main__':
    num_dist_ops_hist()
    ops()
    query_length()
    new_tables_for_users()
    lifetime()
    viewdepth_hist()
    queries_per_table()
    # query_length_hist()
    # num_ops()
    # num_dist_ops()
    Q_vs_D()
    # query_entropy()
    # viewdepth()
    # complexity()
    # cumulative_q_t()
    # # table_touch()
    # # column_touch()
    # # runtime()
    # # new_tables()
